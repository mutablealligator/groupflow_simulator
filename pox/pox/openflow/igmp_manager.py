#!/usr/bin/python
# -*- coding: utf-8 -*-

#  Copyright 2014 Alexander Craig
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
A POX module implementation providing IGMP v3 Multicast Router functionality.

This module does not support any command line arguments. All IGMP parameters are set to RFC
recommended defaults (see RFC 3376).

Depends on openflow.discovery, misc.groupflow_event_tracer (optional)

Created on July 16, 2013

Author: Alexander Craig - alexcraig1@gmail.com
"""

from collections import defaultdict

# POX dependencies
from pox.openflow.discovery import Discovery
from pox.core import core
from pox.lib.revent import *
from pox.misc.groupflow_event_tracer import *
from pox.lib.util import dpid_to_str
import pox.lib.packet as pkt
from pox.lib.packet.igmpv3 import *   # Required for various IGMP variable constants
from pox.lib.packet.ethernet import *
import pox.openflow.libopenflow_01 as of
from pox.lib.addresses import IPAddr, EthAddr
from pox.lib.recoco import Timer
import time

log = core.getLogger()

def int_to_filter_mode_str(filter_mode):
    """Converts an IGMP integer filter mode into the associated string constant."""
    if filter_mode == MODE_IS_INCLUDE:
        return 'MODE_IS_INCLUDE'
    elif filter_mode == MODE_IS_EXCLUDE:
        return 'MODE_IS_EXCLUDE'
    elif filter_mode == CHANGE_TO_INCLUDE_MODE:
        return 'CHANGE_TO_INCLUDE_MODE'
    elif filter_mode == CHANGE_TO_EXCLUDE_MODE:
        return 'CHANGE_TO_INCLUDE_MODE'
    elif filter_mode == ALLOW_NEW_SOURCES:
        return 'ALLOW_NEW_SOURCES'
    elif filter_mode == BLOCK_OLD_SOURCES:
        return 'BLOCK_OLD_SOURCES'
    return 'UNKNOWN_FILTER_MODE'

class MulticastGroupEvent(Event):

    """Event which represents the desired reception state for all interfaces/multicast groups on a single router.
    
    This class contains the following public attributes:
    
    * desired_reception - Records the desired reception state of the router with DPID router_dpid, and
      is formatted as a two dimensional map of lists:
    
    ::
        
        desired_reception[multicast_address][port_index] = [list of desired source addresses]
    
    An empty list in any map entry specifies that reception is desired from all available sources.
    """
    
    def __init__ (self, router_dpid, desired_reception, igmp_trace_event = None):
        Event.__init__(self)
        self.router_dpid = router_dpid
        self.igmp_trace_event = igmp_trace_event
        
        # self.desired_reception[multicast_address][port_index] = [list of desired sources]
        # An empty list indicates reception from all sources is desired
        self.desired_reception = desired_reception
        
    def debug_str(self):
        debug_str = '\n===== MulticastGroupEvent: Router: ' + dpid_to_str(self.router_dpid)
        for mcast_address in self.desired_reception:
            debug_str += '\nMulticast Group: ' + str(mcast_address)
            for port_index in self.desired_reception[mcast_address]:
                debug_str += '\nPort: ' + str(port_index)
                if len(self.desired_reception[mcast_address][port_index]) == 0:
                    debug_str += ' - Reception from all sources requested.'
                    continue
                    
                for address in self.desired_reception[mcast_address][port_index]:
                    debug_str += '\nDesired Source: ' + str(address)
        return debug_str + '\n===== MulticastGroupEvent'
        
    
class MulticastTopoEvent(Event):

    """Event which signifies a change in topology that will be relevant to the multicast routing module.
    
    This class contains the following public attributes:
    
    * event_type: One of LINK_UP (0) or LINK_DOWN (1). Records which type of topology change triggered this event.
    * link_changes: A list of tuples specifying which links changed state (type of state change determined by event_type).
      The tuples are formatted as (Egress Router DPID, Ingress Router DPID, Egress Router Port)
    * adjacency_map: A map of all adjacencies between switches learned by the IGMP module, formatted as follows:
    
    ::
        
        adjacency_map[egress_router_dpid][ingress_router_dpid] = egress_router_port
    
    """
    
    LINK_UP     = 0
    LINK_DOWN   = 1
    
    def __init__ (self, event_type, link_changes, adjacency_map):
        Event.__init__(self)
        self.event_type = event_type
        self.link_changes = link_changes
        self.adjacency_map = adjacency_map
    
    def debug_str(self):
        debug_str = '\n===== MulticastTopoEvent - Type: '
        if self.event_type == MulticastTopoEvent.LINK_UP:
            debug_str += 'LINK_UP'
        elif self.event_type == MulticastTopoEvent.LINK_DOWN:
            debug_str += 'LINK_DOWN'
        
        debug_str += '\nChanged Links:'
        for link in self.link_changes:
            debug_str += '\n' + dpid_to_str(link[0]) + ' -> ' + dpid_to_str(link[1]) + ' Port: ' + str(link[2])
        return debug_str + '\n===== MulticastTopoEvent'
    
    
class MulticastMembershipRecord:
    """Class representing the group record state maintained by an IGMPv3 multicast router

    Multicast routers implementing IGMPv3 keep state per group per attached network.  This group state consists of a
    filter-mode, a list of sources, and various timers.  For each attached network running IGMP, a multicast router
    records the desired reception state for that network.  That state conceptually consists of a set of records of the
    form:
    
    ::
        
        (multicast address, group timer, filter-mode, (source records))
    
    Each source record is of the form:
    
    ::
        
        (source address, source timer)
    
    """
    
    def __init__(self, mcast_address, timer_value):
        self.multicast_address = mcast_address
        self.group_timer = timer_value
        self.filter_mode = MODE_IS_INCLUDE  # TODO: Re-examine this as the default
        self.x_source_records = [] # Source records are stored as a list of the source address and the source timer
        self.y_source_records = [] # Source records with a zero source timer are stored separately
    
    def get_curr_source_timer(self, ip_addr):
        """Returns the current source timer for the specified IP address, or 0 if the specified IP is not known by this group record."""
        for record in self.x_source_records:
            if record[0] == ip_addr:
                return record[1]
         
        return 0
        
    def get_x_addr_set(self):
        """Returns the set of addresses in the X set of source records (see RFC 3376)
        
        Note: When in INCLUDE mode, all sources are stored in the X set.
        """
        
        return_set = set()
        for record in self.x_source_records:
            return_set.add(record[0])
        return return_set
    
    def get_y_addr_set(self):
        """Returns the set of addresses in the Y set of source records (see RFC 3376)
        
        Note: When in INCLUDE mode, his set should always be empty.
        """
        return_set = set()
        for record in self.y_source_records:
            return_set.add(record[0])
        return return_set
    
    def remove_source_record(self, ip_addr):
        """Removes the source record with the specified IP address from the group record"""
        for record in self.x_source_records:
            if record[0] == ip_addr:
                self.x_source_records.remove(record)
                return
        
        for record in self.y_source_records:
            if record[0] == ip_addr:
                self.y_source_records.remove(record)
                return



class IGMPv3Router(EventMixin):
    """Class representing an IGMP v3 router, with IGMP functionality implemented through OpenFlow interaction."""

    def __init__(self, manager):
        self.connection = None
        self.ports = None # Complete list of all ports on the router, contains complete port object provided by connection objects
        self.igmp_ports = [] # List of ports over which IGMP service should be provided, contains only integer indexes
        
        # self.multicast_records[igmp_port_index][multicast_address]
        self.multicast_records = defaultdict(lambda : defaultdict(lambda : None))
        self.dpid = None
        self.igmp_manager = manager
        self._listeners = None
        self._connected_at = None
        self.prev_desired_reception = None

    def __repr__(self):
        return dpid_to_str(self.dpid)

    def ignore_connection(self):
        if self.connection is not None:
            log.debug('Disconnect %s' % (self.connection, ))
            self.connection.removeListeners(self._listeners)
            self.connection = None
            self._listeners = None

    def listen_on_connection(self, connection):
        if self.dpid is None:
            self.dpid = connection.dpid
        assert self.dpid == connection.dpid
        if self.ports is None:
            self.ports = connection.features.ports
            for port in self.ports:
                if port.port_no ==  of.OFPP_CONTROLLER or port.port_no == of.OFPP_LOCAL:
                    continue
                self.igmp_ports.append(port.port_no)
            # log.debug(self.ports) # Debug
        # self.disconnect()
        log.debug('Connect %s' % (connection, ))
        self.connection = connection
        self._listeners = self.listenTo(connection)
        self._connected_at = time.time()
        
        # Add a flow mod which sends complete IGMP packets to the controller
        msg = of.ofp_flow_mod()
        msg.match.dl_type = 0x0800 # IPv4
        msg.match.nw_proto = IGMP_PROTOCOL # IGMP
        msg.hard_timeout = 0
        msg.idle_timeout = 0
        msg.actions.append(of.ofp_action_output(port = of.OFPP_CONTROLLER))
        connection.send(msg)

    def _handle_ConnectionDown(self, event):
        self.ignore_connection()
        
    def debug_print_group_records(self):
        log.debug(' ')
        log.debug('== Router ' + str(self) + ' Group Membership State ==')
        for port in self.multicast_records:
            log.debug('Port: ' + str(port))
            for mcast_address in self.multicast_records[port]:
                group_record = self.multicast_records[port][mcast_address]
                if group_record is None:    # Protects against mutual exclusion issues
                    continue
                
                if group_record.filter_mode == MODE_IS_EXCLUDE:
                    log.debug(str(group_record.multicast_address) + ' - ' 
                            + int_to_filter_mode_str(group_record.filter_mode)
                            + ' - Timer: ' + str(group_record.group_timer))
                else:
                    log.debug(str(group_record.multicast_address) + ' - ' 
                            + int_to_filter_mode_str(group_record.filter_mode))
                for source_record in group_record.x_source_records:
                    log.debug('X: ' + str(source_record[0]) + ' - Timer: ' + str(source_record[1]))
                for source_record in group_record.y_source_records:
                    log.debug('Y: ' + str(source_record[0]) + ' - Timer: ' + str(source_record[1]))
        log.debug('=====================================================')
        log.debug(' ')
    
    def update_desired_reception_state(self, igmp_trace_event = None):
        """Updates the object's cached map of desired reception state, and generates a MulticastGroupEvent if state changed."""
        desired_reception = defaultdict(lambda : defaultdict(lambda : None))
        
        for port_index in self.multicast_records:
            for mcast_address in self.multicast_records[port_index]:
                group_record = self.multicast_records[port_index][mcast_address]
                if group_record.filter_mode == MODE_IS_INCLUDE:
                    for source_record in group_record.x_source_records:
                        if desired_reception[mcast_address][port_index] == None:
                            desired_reception[mcast_address][port_index] = []
                        desired_reception[mcast_address][port_index].append(source_record[0])
                
                if group_record.filter_mode == MODE_IS_EXCLUDE:
                    if len(group_record.x_source_records) == 0:
                        desired_reception[mcast_address][port_index] = []
                        continue
                        
                    for source_record in group_record.x_source_records:
                        if desired_reception[mcast_address][port_index] == None:
                            desired_reception[mcast_address][port_index] = []
                        desired_reception[mcast_address][port_index].append(source_record[0])
        
        if not igmp_trace_event is None:
                igmp_trace_event.set_igmp_end_time()
                core.groupflow_event_tracer.archive_trace_event(igmp_trace_event)
        
        if self.prev_desired_reception == None:
            event = MulticastGroupEvent(self.dpid, desired_reception, igmp_trace_event)
            self.igmp_manager.raiseEvent(event)
        else:
            if desired_reception == self.prev_desired_reception:
                log.debug('No change in reception state.')
            else:
                event = MulticastGroupEvent(self.dpid, desired_reception, igmp_trace_event)
                self.igmp_manager.raiseEvent(event)
            
        self.prev_desired_reception = desired_reception
    
    def create_group_record(self, event, igmp_group_record, group_timer):
        """Creates a group record from the specific PacketIn event and associated igmp_group_record read from the packet.

        If the record did not already exist it is initialized with the provided group timer. If it
        did exist, the existing record IS NOT modified.
        """
        
        if self.multicast_records[event.port][igmp_group_record.multicast_address] is None:
            self.multicast_records[event.port][igmp_group_record.multicast_address] = \
                    MulticastMembershipRecord(igmp_group_record.multicast_address, group_timer)
            log.debug('Added group record for multicast IP: ' + str(igmp_group_record.multicast_address))
        
        return self.multicast_records[event.port][igmp_group_record.multicast_address]
    
    def send_group_specific_query(self, port, multicast_address, group_record, retransmissions = -1):
        """Generates a group specific query, and sends a PacketOut message to deliver it to the specified port"""
        if self.connection is None:
            log.warn('Unable to access connection with switch: ' + dpid_to_str(self.dpid))
            return
            
        if retransmissions == -1:
            retransmissions = self.igmp_manager.igmp_last_member_query_count - 1
            
        # Build the IGMP payload for the message
        igmp_pkt = pkt.igmpv3()
        igmp_pkt.ver_and_type = MEMBERSHIP_QUERY
        igmp_pkt.max_response_time = self.igmp_manager.igmp_query_response_interval
        igmp_pkt.address = multicast_address
        igmp_pkt.qrv = self.igmp_manager.igmp_robustness
        igmp_pkt.qqic = self.igmp_manager.igmp_query_interval
        igmp_pkt.dlen = 12  # TODO: This should be determined by the IGMP packet class somehow
        if group_record.group_timer > self.igmp_manager.igmp_last_member_query_time / 10:
            igmp_pkt.suppress_router_processing = True
        else:
            igmp_pkt.suppress_router_processing = False
        eth_pkt = self.igmp_manager.encapsulate_igmp_packet(igmp_pkt)
        output = of.ofp_packet_out(action = of.ofp_action_output(port=port))
        output.data = eth_pkt.pack()
        output.pack()
        self.connection.send(output)
        log.info('Router ' + str(self) + ':' + str(port) + '| Sent group specific query for group: ' + str(multicast_address))
        group_record.group_timer = self.igmp_manager.igmp_last_member_query_time / 10
        
        if retransmissions > 0:
            log.debug('Retransmissions remaininig: ' + str(retransmissions))
            Timer(self.igmp_manager.igmp_last_member_query_interval / 10, self.send_group_specific_query, 
                    args = [port, multicast_address, group_record, retransmissions - 1], recurring = False)
                    
    def send_group_and_source_specific_query(self, port, multicast_address, group_record, sources, retransmissions = -1):
        """Generates a group and source specific query, and sends a PacketOut message to deliver it to the specified port"""
        # TODO: Retransmissions may not properly handle the source timers, as they will be based on the timers
        # at the time of retransmission
        if self.connection is None:
            log.warn('Unable to access connection with switch: ' + dpid_to_str(self.dpid))
            return
        
        if retransmissions == -1:
            retransmissions = self.igmp_manager.igmp_last_member_query_count - 1
            
        # Build and send the IGMP packet for sources whose source timer is greater than LMQT
        igmp_pkt = pkt.igmpv3()
        igmp_pkt.ver_and_type = MEMBERSHIP_QUERY
        igmp_pkt.max_response_time = self.igmp_manager.igmp_query_response_interval
        igmp_pkt.address = multicast_address
        igmp_pkt.qrv = self.igmp_manager.igmp_robustness
        igmp_pkt.qqic = self.igmp_manager.igmp_query_interval
        igmp_pkt.dlen = 12  # TODO: This should be determined by the IGMP packet class somehow
        for source in sources:
            if group_record.get_curr_source_timer(source) > self.igmp_manager.igmp_last_member_query_time / 10:
                igmp_pkt.num_sources += 1
                igmp_pkt.source_addresses.append(source)
        igmp_pkt.suppress_router_processing = True
        if(igmp_pkt.num_sources > 0):
            eth_pkt = self.igmp_manager.encapsulate_igmp_packet(igmp_pkt)
            output = of.ofp_packet_out(action = of.ofp_action_output(port=port))
            output.data = eth_pkt.pack()
            output.pack()
            self.connection.send(output)
            log.info('Router ' + str(self) + ':' + str(port) + '| Sent group/source specific query with router suppression for group: ' + str(multicast_address))
            for source in igmp_pkt.source_addresses:
                log.info('Source: ' + str(source))
                
        # Build and send the IGMP packet for sources whose source timer is less than LMQT
        igmp_pkt = pkt.igmpv3()
        igmp_pkt.ver_and_type = MEMBERSHIP_QUERY
        igmp_pkt.max_response_time = self.igmp_manager.igmp_query_response_interval
        igmp_pkt.address = multicast_address
        igmp_pkt.qrv = self.igmp_manager.igmp_robustness
        igmp_pkt.qqic = self.igmp_manager.igmp_query_interval
        igmp_pkt.dlen = 12  # TODO: This should be determined by the IGMP packet class somehow
        for source in sources:
            if group_record.get_curr_source_timer(source) <= self.igmp_manager.igmp_last_member_query_time / 10:
                igmp_pkt.num_sources += 1
                igmp_pkt.source_addresses.append(source)
        igmp_pkt.suppress_router_processing = False
        if(igmp_pkt.num_sources > 0):
            eth_pkt = self.igmp_manager.encapsulate_igmp_packet(igmp_pkt)
            output = of.ofp_packet_out(action = of.ofp_action_output(port=port))
            output.data = eth_pkt.pack()
            output.pack()
            self.connection.send(output)
            log.info('Router ' + str(self) + ':' + str(port) + '| Sent group/source specific query without router suppression for group: ' + str(multicast_address))
            
            # Update timers for all querried source records to LMQT
            for source_record in group_record.x_source_records:
                if source_record[0] in igmp_pkt.source_addresses:
                    source_record[1] = self.igmp_manager.igmp_last_member_query_time / 10
            source_records_to_move = []
            for source_record in group_record.y_source_records:
                if source_record[0] in igmp_pkt.source_addresses:
                    source_records_to_move.append(source_record)
            for source_record in source_records_to_move:
                group_record.y_source_records.remove(source_record)
                group_record.x_source_records.append([source_record[0], self.igmp_manager.igmp_last_member_query_time / 10])
            
            # Debug print
            for source in igmp_pkt.source_addresses:
                log.info('Source: ' + str(source))
        
        if retransmissions > 0:
            log.debug('Retransmissions remaininig: ' + str(retransmissions))
            Timer(self.igmp_manager.igmp_last_member_query_interval / 10, self.send_group_and_source_specific_query, 
                    args = [port, multicast_address, group_record, sources, retransmissions - 1], recurring = False)
        
    
    def remove_group_record(self, port, multicast_address):
        """Removes the group record identified by the provided port and multicast_address from the router's set of records.

        Does nothing if the record did not exist.
        """
        
        if not self.multicast_records[port]:
            # KLUDGE: This ugly line is here because the act of checking self.multicast_records[port]
            # creates a default dict with no entries even if one did not exist before... should probably
            # find a better way to deal with this
            del self.multicast_records[port]
            return
            
        if not self.multicast_records[port][multicast_address] is None:
            del self.multicast_records[port][multicast_address]
            if not self.multicast_records[port]:
                # No group records are being stored for this interface
                del self.multicast_records[port]
    
    def process_state_change_record(self, event, igmp_group_record):
        """Processes state change IGMP membership reports according to the following table (See RFC 3376):

        +--------------+--------------+-------------------+-----------------------+
        | Router State | Report Rec'd | New Router State  |   Actions             |
        +==============+==============+===================+=======================+
        | INCLUDE (A)  | ALLOW (B)    | INCLUDE (A+B)     |   (B)=GMI             |
        +--------------+--------------+-------------------+-----------------------+
        | INCLUDE (A)  |  BLOCK (B)   | INCLUDE (A)       |   Send Q(G,A*B)       |
        +--------------+--------------+-------------------+-----------------------+
        | INCLUDE (A)  | TO_EX (B)    | EXCLUDE (A*B,B-A) |   (B-A)=0,            |
        |              |              |                   |   Delete (A-B),       |
        |              |              |                   |   Send Q(G,A*B),      |
        |              |              |                   |   Group Timer=GMI     |
        +--------------+--------------+-------------------+-----------------------+
        | INCLUDE (A)  | TO_IN (B)    | INCLUDE (A+B)     |   (B)=GMI,            |
        |              |              |                   |   Send Q(G,A-B)       |
        +--------------+--------------+-------------------+-----------------------+
        | EXCLUDE (X,Y)| ALLOW (A)    | EXCLUDE (X+A,Y-A) |   (A)=GMI             |
        +--------------+--------------+-------------------+-----------------------+
        | EXCLUDE (X,Y)| BLOCK (A)    | EXCLUDE(X+(A-Y),Y)|   (A-X-Y)=Group Timer,|
        |              |              |                   |   Send Q(G,A-Y)       |
        +--------------+--------------+-------------------+-----------------------+
        | EXCLUDE (X,Y)| TO_EX (A)    | EXCLUDE (A-Y,Y*A) |   (A-X-Y)=Group Timer,|
        |              |              |                   |   Delete (X-A),       |
        |              |              |                   |   Delete (Y-A),       |
        |              |              |                   |   Send Q(G,A-Y),      |
        |              |              |                   |   Group Timer=GMI     |
        +--------------+--------------+-------------------+-----------------------+
        | EXCLUDE (X,Y)| TO_IN (A)    | EXCLUDE (X+A,Y-A) |   (A)=GMI,            |
        |              |              |                   |   Send Q(G,X-A),      |
        |              |              |                   |   Send Q(G)           |
        +--------------+--------------+-------------------+-----------------------+

        Note: When the group is in INCLUDE mode, the set of addresses is stored in the same list as the X set when
        in EXCLUDE mode

        """
        router_group_record = self.create_group_record(event, igmp_group_record,
                    self.igmp_manager.igmp_group_membership_interval)
        new_x_source_records = []
        new_y_source_records = []
        igmp_record_addresses = igmp_group_record.get_addr_set()
        
        if router_group_record.filter_mode == MODE_IS_INCLUDE:
            if igmp_group_record.record_type == ALLOW_NEW_SOURCES:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received ALLOW_NEW_SOURCES')
                new_x_set = router_group_record.get_x_addr_set() | igmp_record_addresses
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.igmp_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                        
                router_group_record.x_source_records = new_x_source_records
                router_group_record.y_source_records = new_y_source_records
            
            elif igmp_group_record.record_type == BLOCK_OLD_SOURCES:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received BLOCK_OLD_SOURCES')
                
                query_addr_set = router_group_record.get_x_addr_set() & igmp_record_addresses
                
                # Send: Q(G, A*B)
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, router_group_record, query_addr_set)
                
            elif igmp_group_record.record_type == CHANGE_TO_EXCLUDE_MODE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received CHANGE_TO_EXCLUDE_MODE')
                router_group_record.filter_mode = MODE_IS_EXCLUDE
                
                new_x_set = router_group_record.get_x_addr_set() & igmp_record_addresses
                new_y_set = igmp_record_addresses - router_group_record.get_x_addr_set()
                for address in new_x_set:
                    new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                
                router_group_record.x_source_records = new_x_source_records
                router_group_record.y_source_records = new_y_source_records
                router_group_record.group_timer = self.igmp_manager.igmp_group_membership_interval
                
                # Send: Q(G, A*B)
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, router_group_record, new_x_set)
                
            elif igmp_group_record.record_type == CHANGE_TO_INCLUDE_MODE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received CHANGE_TO_INCLUDE_MODE')
                
                new_x_set = router_group_record.get_x_addr_set() | igmp_record_addresses
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.igmp_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                
                query_addr_set = router_group_record.get_x_addr_set() - igmp_record_addresses
                
                router_group_record.x_source_records = new_x_source_records
                router_group_record.y_source_records = new_y_source_records
                    
                # Send Q(G,A-B)
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, router_group_record, query_addr_set)

        elif router_group_record.filter_mode == MODE_IS_EXCLUDE:
            if igmp_group_record.record_type == ALLOW_NEW_SOURCES:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received ALLOW_NEW_SOURCES')
                new_x_set = router_group_record.get_x_addr_set() | igmp_record_addresses
                new_y_set = router_group_record.get_y_addr_set() - igmp_record_addresses
                
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.igmp_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                
                router_group_record.x_source_records = new_x_source_records
                router_group_record.y_source_records = new_y_source_records
                
            elif igmp_group_record.record_type == BLOCK_OLD_SOURCES:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received BLOCK_OLD_SOURCES')
                new_x_set = router_group_record.get_x_addr_set() | (igmp_record_addresses - router_group_record.get_y_addr_set())
                new_y_set = router_group_record.get_y_addr_set()
                group_timer_set = (igmp_record_addresses - router_group_record.get_x_addr_set()) - router_group_record.get_y_addr_set()
                query_addr_set = igmp_record_addresses - router_group_record.get_y_addr_set()
                
                for address in new_x_set:
                    if address in group_timer_set:
                        new_x_source_records.append([address, router_group_record.group_timer])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                
                router_group_record.x_source_records = new_x_source_records
                router_group_record.y_source_records = new_y_source_records
                
                # Send Q(G, A-Y)
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, router_group_record, query_addr_set)
                
            elif igmp_group_record.record_type == CHANGE_TO_EXCLUDE_MODE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received CHANGE_TO_EXCLUDE_MODE')
                
                new_x_set = igmp_record_addresses - router_group_record.get_y_addr_set()
                new_y_set = router_group_record.get_y_addr_set() & igmp_record_addresses
                group_timer_set = (igmp_record_addresses - router_group_record.get_x_addr_set()) - router_group_record.get_y_addr_set()
                
                for address in new_x_set:
                    if address in group_timer_set:
                        new_x_source_records.append([address, router_group_record.group_timer])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                
                for address in new_y_set:
                    new_y_source_records.append([address, 0])

                router_group_record.group_timer = self.igmp_manager.igmp_group_membership_interval
                router_group_record.x_source_records = new_x_source_records
                router_group_record.y_source_records = new_y_source_records
                
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, router_group_record, new_x_set)
                    
            elif igmp_group_record.record_type == CHANGE_TO_INCLUDE_MODE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received CHANGE_TO_INCLUDE_MODE')
                
                new_x_set = router_group_record.get_x_addr_set() | igmp_record_addresses
                new_y_set = router_group_record.get_y_addr_set() - igmp_record_addresses
                query_addr_set = router_group_record.get_x_addr_set() - igmp_record_addresses
                
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.igmp_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                
                router_group_record.x_source_records = new_x_source_records
                router_group_record.y_source_records = new_y_source_records
                
                # Send Q(G, X-A)
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, router_group_record, query_addr_set)
                # Send Q(G)
                self.send_group_specific_query(event.port, igmp_group_record.multicast_address, router_group_record)
                    
                    
        if router_group_record.filter_mode == MODE_IS_INCLUDE and len(new_x_source_records) == 0:
            self.remove_group_record(event.port, igmp_group_record.multicast_address)
    
    def process_current_state_record(self, event, igmp_group_record):
        """Processes current state IGMP membership reports according to the following table (See RFC 3376):

        +--------------+--------------+--------------------+----------------------+
        | Router State | Report Rec'd | New Router State   |     Actions          |
        +==============+==============+====================+======================+
        | INCLUDE (A)  | IS_IN (B)    | INCLUDE (A+B)      |     (B)=GMI          |
        +--------------+--------------+--------------------+----------------------+
        | INCLUDE (A)  | IS_EX (B)    | EXCLUDE (A*B,B-A)  |    (B-A)=0,          |
        |              |              |                    |    Delete (A-B),     |
        |              |              |                    |    Group Timer=GMI   |
        +--------------+--------------+--------------------+----------------------+
        | EXCLUDE (X,Y)| IS_IN (A)    | EXCLUDE (X+A,Y-A)  |    (A)=GMI           |
        +--------------+--------------+--------------------+----------------------+
        | EXCLUDE (X,Y)| IS_EX (A)    | EXCLUDE (A-Y,Y*A)  |    (A-X-Y)=GMI,      |
        |              |              |                    |    Delete (X-A),     |
        |              |              |                    |    Delete (Y-A),     |
        |              |              |                    |    Group Timer=GMI   |
        +--------------+--------------+--------------------+----------------------+

        Note: When the group is in INCLUDE mode, the set of addresses is stored in
        the same list as the X set when in EXCLUDE mode

        """
        
        router_group_record = self.create_group_record(event, igmp_group_record,
                    self.igmp_manager.igmp_group_membership_interval)
        new_x_source_records = []
        new_y_source_records = []
        igmp_record_addresses = igmp_group_record.get_addr_set()
        
        if router_group_record.filter_mode == MODE_IS_INCLUDE:
            if igmp_group_record.record_type == MODE_IS_INCLUDE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received MODE_IS_INCLUDE')
                
                new_x_set = router_group_record.get_x_addr_set() | igmp_record_addresses
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.igmp_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                
            elif igmp_group_record.record_type == MODE_IS_EXCLUDE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received MODE_IS_EXCLUDE')
                router_group_record.filter_mode = MODE_IS_EXCLUDE
                
                new_x_set = router_group_record.get_x_addr_set() & igmp_record_addresses
                new_y_set = igmp_record_addresses - router_group_record.get_x_addr_set()
                for address in new_x_set:
                    new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                router_group_record.group_timer = self.igmp_manager.igmp_group_membership_interval
                
        elif router_group_record.filter_mode == MODE_IS_EXCLUDE:
            if igmp_group_record.record_type == MODE_IS_INCLUDE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received MODE_IS_INCLUDE')
                new_x_set = router_group_record.get_x_addr_set() | igmp_record_addresses
                new_y_set = router_group_record.get_y_addr_set() - igmp_record_addresses
                
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.igmp_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                    
                            
            elif igmp_group_record.record_type == MODE_IS_EXCLUDE:
                log.info(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received MODE_IS_EXCLUDE')
                
                new_x_set = igmp_record_addresses - router_group_record.get_y_addr_set()
                new_y_set = router_group_record.get_y_addr_set() & igmp_record_addresses
                gmi_set = (igmp_record_addresses - router_group_record.get_x_addr_set()) - router_group_record.get_y_addr_set()
                
                for address in new_x_set:
                    if address in gmi_set:
                        new_x_source_records.append([address, self.igmp_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                
                router_group_record.group_timer = self.igmp_manager.igmp_group_membership_interval

        # Prune any INCLUDE_MODE records which do not specify any sources
        if router_group_record.filter_mode == MODE_IS_INCLUDE and len(new_x_source_records) == 0:
            self.remove_group_record(event.port, igmp_group_record.multicast_address)
        else:
            router_group_record.x_source_records = new_x_source_records
            router_group_record.y_source_records = new_y_source_records
    
    def process_igmp_event(self, event, igmp_trace_event = None):
        """Processes any IGMP event received by the router."""
        
        igmp_pkt = event.parsed.find(pkt.igmpv3)
        
        if igmp_pkt.msg_type == MEMBERSHIP_REPORT_V3:
            if not igmp_trace_event is None:
                igmp_trace_event.set_igmp_start_time(event)
                
            for igmp_group_record in igmp_pkt.group_records:                
                if igmp_group_record.record_type == MODE_IS_INCLUDE or \
                        igmp_group_record.record_type == MODE_IS_EXCLUDE:
                    self.process_current_state_record(event, igmp_group_record)
                            
                elif igmp_group_record.record_type == CHANGE_TO_INCLUDE_MODE or \
                        igmp_group_record.record_type == CHANGE_TO_EXCLUDE_MODE or \
                        igmp_group_record.record_type == ALLOW_NEW_SOURCES or \
                        igmp_group_record.record_type == BLOCK_OLD_SOURCES:
                    self.process_state_change_record(event, igmp_group_record)
                    
            # Debug - Print a listing of the current group membership state after
            # all group records are processed
            self.debug_print_group_records()
            self.update_desired_reception_state(igmp_trace_event)
                
        elif igmp_pkt.msg_type == MEMBERSHIP_QUERY_V3 and igmp_pkt.self.suppress_router_processing == False \
                and igmp_pkt.address != IPAddr("0.0.0.0"):
            router_group_record = self.group_records[event.port][igmp_pkt.address]
            if router_group_record == None:
                return

            # Reset timers:
            # For a group specific query, reset the group timer for the group to LMQT
            if igmp_pkt.num_sources == 0:
                log.debug(str(self) + ':' + str(event.port) + '| Got group specific query for ' + str(igmp_group_record.multicast_address))
                router_group_record.group_timer = self.igmp_manager.igmp_last_member_query_time
            # For a group and source specific query, reset the source timer for each included
            # source to LMQT
            else:
                log.debug(str(self) + ':' + str(event.port) + '| Got group and source specific query for ' + str(igmp_group_record.multicast_address))
                for source_record in router_group_record.x_source_records:
                    if source_record[0] in igmp_pkt.source_addresses:
                        source_record[1] = self.igmp_manager.igmp_last_member_query_time / 10
                source_records_to_move = []
                for source_record in router_group_record.y_source_records:
                    if source_record[0] in igmp_pkt.source_addresses:
                        source_records_to_move.append(source_record)
                for source_record in source_records_to_move:
                    router_group_record.y_source_records.remove(source_record)
                    router_group_record.x_source_records.append([source_record[0], 
                            self.igmp_manager.igmp_last_member_query_time / 10])
        
        


class IGMPManager(EventMixin):
    
    """Class which stores global IGMP settings, and manages a map of IGMPv3Router objects to implement IGMP support for OpenFlow switches."""
    
    _eventMixin_events = set([
        MulticastGroupEvent,
        MulticastTopoEvent
    ])
    
    _core_name = "openflow_igmp_manager"

    def __init__(self):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self, priority=100)
            core.openflow_discovery.addListeners(self, priority=100)
        
        # Set variables for IGMP operation to default values
        self.igmp_robustness = 2
        self.igmp_query_interval = 125               # Seconds
        # Debug: Make the IGMP query interval shorter than default to aid testing
        # self.igmp_query_interval = 20
        self.igmp_query_response_interval = 100     # Tenths of a second
        self.igmp_group_membership_interval = (self.igmp_robustness * self.igmp_query_interval) \
                + (self.igmp_query_response_interval * 0.1)             # Seconds
        # Note: Other querier present interval should not actually be required with this centralized
        # implementation
        self.igmp_other_querier_present_interval = (self.igmp_robustness * self.igmp_query_interval) \
                + ((self.igmp_query_response_interval * 0.1) / 2)       # Seconds
        self.igmp_startup_query_interval = self.igmp_query_interval / 4  # Seconds
        self.igmp_startup_query_count = self.igmp_robustness
        self.igmp_last_member_query_interval = 10                       # Tenths of a second
        self.igmp_last_member_query_count = self.igmp_robustness
        self.igmp_last_member_query_time = self.igmp_last_member_query_interval \
                * self.igmp_last_member_query_count                     # Tenths of a second
        self.igmp_unsolicited_report_interval = 1       # Seconds (not used in router implementation)

        # Setup topology discovery state
        self.got_first_connection_up = False
        
        # Known routers:  [dpid] -> Router
        self.routers = {}
        # Adjacency map:  [router_dpid_1][router_dpid_2] -> port from router1 to router2
        self.adjacency = defaultdict(lambda : defaultdict(lambda : \
                None))

        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'))
    
    def decrement_all_timers(self):
        """Decrements the source and group timers for all group_records in the network, and transitions any state as necessary.

        As long as this is always called from a recoco Timer, mutual exclusion should not be an issue.
        """
        for router_dpid in self.routers:
            router = self.routers[router_dpid]
            records_modified = False
            ports_to_delete = []
            for port in router.multicast_records:
                mcast_addresses_to_delete = []
                
                for mcast_address in router.multicast_records[port]:
                    group_record = router.multicast_records[port][mcast_address]
                    
                    if group_record is None:        # Protects against mutual exclusion issues
                        continue
                    
                    source_records_to_move = []
                    for source_record in group_record.x_source_records:
                        if source_record[1] > 0:
                            source_record[1] -= 1
                        
                        if group_record.filter_mode == MODE_IS_EXCLUDE and source_record[1] == 0:
                            source_records_to_move.append(source_records)
                            records_modified = True
                            
                    for source_record in source_records_to_move:
                        group_record.x_source_records.remove(source_record)
                        group_record.y_source_records.append(source_record)
                    
                    if group_record.filter_mode == MODE_IS_EXCLUDE:
                        if group_record.group_timer > 0:
                            group_record.group_timer -= 1
                            
                        if group_record.group_timer == 0:
                            log.debug('Group Timer expired')
                            # Switch the group record back to INCLUDE mode
                            group_record.filter_mode = MODE_IS_INCLUDE
                            group_record.y_source_records = []
                            if not group_record.x_source_records:
                                mcast_addresses_to_delete.append(mcast_address)
                                records_modified = True
                
                for mcast_address in mcast_addresses_to_delete:
                    del router.multicast_records[port][mcast_address]
                    
                if not router.multicast_records[port]:
                    ports_to_delete.append(port)
             
            for port in ports_to_delete:
                del router.multicast_records[port]
            
            if records_modified:
                router.update_desired_reception_state()

        
    def encapsulate_igmp_packet(self, igmp_pkt):
        """Encapsulates the provided IGMP packet into IP and Ethernet packets, and returns the encapsulating ethernet packet"""
        
        # Build the encapsulating IP packet
        ip_pkt = pkt.ipv4()
        ip_pkt.ttl = 1
        ip_pkt.protocol = IGMP_PROTOCOL
        ip_pkt.tos = 0xc0   # Type of service: Internetwork Control
        ip_pkt.dstip = IGMP_V3_ALL_SYSTEMS_ADDRESS
        # TODO: Set IP Router alert option
        ip_pkt.payload = igmp_pkt
        
        # Build the encapsulating ethernet packet
        eth_pkt = eth = pkt.ethernet(type=pkt.ethernet.IP_TYPE)
        eth_pkt.dst = ETHER_BROADCAST
        eth_pkt.payload = ip_pkt
        
        return eth_pkt
     
    def send_igmp_query_to_all_networks(self, igmp_pkt):
        """Encapsulates the provided IGMP packet into IP and Ethernet packets, and sends the packet to all attached network on all routers."""
        # Build the encapsulating IP/ethernet packet
        eth_pkt = self.encapsulate_igmp_packet(igmp_pkt)
        
        # Send out the packet
        for router_dpid in self.routers:
            sending_router = self.routers[router_dpid]
            if sending_router.connection is None:
                log.warn('Unable to access connection with switch: ' + dpid_to_str(router_dpid))
                continue
                
            for port_num in sending_router.igmp_ports:
                output = of.ofp_packet_out(action = of.ofp_action_output(port=port_num))
                output.data = eth_pkt.pack()
                output.pack()
                sending_router.connection.send(output)
                # log.debug('Router ' + str(sending_router) + ' sending IGMP query on port: ' + str(port_num))
        
    def launch_igmp_general_query(self):
        """Generates an IGMP general query, broadcasts it from all ports on all routers."""
        log.debug('Launching IGMP general query from all routers')
        
        # Build the IGMP payload for the message
        igmp_pkt = pkt.igmpv3()
        igmp_pkt.ver_and_type = MEMBERSHIP_QUERY
        igmp_pkt.max_response_time = self.igmp_query_response_interval
        igmp_pkt.address = IPAddr("0.0.0.0")
        igmp_pkt.qrv = self.igmp_robustness
        igmp_pkt.qqic = self.igmp_query_interval
        igmp_pkt.dlen = 12  # TODO: This should be determined by the IGMP packet class somehow
        
        self.send_igmp_query_to_all_networks(igmp_pkt)
        
        
    def drop_packet(self, packet_in_event):
        """Drops the packet represented by the PacketInEvent without any flow table modification."""
        msg = of.ofp_packet_out()
        msg.data = packet_in_event.ofp
        msg.buffer_id = packet_in_event.ofp.buffer_id
        msg.in_port = packet_in_event.port
        msg.actions = []    # No actions = drop packet
        packet_in_event.connection.send(msg)
    
    def add_igmp_router(self, router_dpid, connection):
        """Registers a new router for management by the IGMP module."""
        if not router_dpid in self.routers:
            # New router
            router = IGMPv3Router(self)
            router.dpid = router_dpid
            self.routers[router_dpid] = router
            log.info('Learned new router: ' + dpid_to_str(router.dpid))
            router.listen_on_connection(connection)
        
        if not self.got_first_connection_up:
            self.got_first_connection_up = True
            # Setup the Timer to send periodic general queries
            self.general_query_timer = Timer(self.igmp_query_interval, self.launch_igmp_general_query, recurring = True)
            log.debug('Launching IGMP general query timer with interval ' + str(self.igmp_query_interval) + ' seconds')
            # Setup the timer to handle group and source timers
            self.timer_decrementing_timer = Timer(1, self.decrement_all_timers, recurring = True)

    def _handle_LinkEvent(self, event):
        """Handler for LinkEvents from the discovery module, which are used to learn the network topology."""

        def flip(link):
            return Discovery.Link(link[2], link[3], link[0], link[1])
        
        log.debug('Got LinkEvent: ' + dpid_to_str(event.link.dpid1) + ' --> ' + dpid_to_str(event.link.dpid2) + ' LinkUp: ' + str(not event.removed))
        
        l = event.link
        if not l.dpid1 in self.routers:
            self.add_igmp_router(l.dpid1, core.openflow.getConnection(l.dpid1))
        router1 = self.routers[l.dpid1]
        if not l.dpid2 in self.routers:
            self.add_igmp_router(l.dpid2, core.openflow.getConnection(l.dpid2))
        router2 = self.routers[l.dpid2]

        if event.removed:
            log.warn('Link down: ' + dpid_to_str(l.dpid1) + ' -> ' + dpid_to_str(l.dpid2))
            # This link no longer up
            link_changes = []
            if l.dpid2 in self.adjacency[l.dpid1]:
                link_changes.append((l.dpid1, l.dpid2, self.adjacency[l.dpid1][l.dpid2]))
                del self.adjacency[l.dpid1][l.dpid2]
            if l.dpid1 in self.adjacency[l.dpid2]:
                link_changes.append((l.dpid2, l.dpid1, self.adjacency[l.dpid2][l.dpid1]))
                del self.adjacency[l.dpid2][l.dpid1]
            router1.igmp_ports.append(l.port1)
            router2.igmp_ports.append(l.port2)
                
            log.info('Removed adjacency: ' + str(router1) + '.'
                 + str(l.port1) + ' <-> ' + str(router2) + '.'
                 + str(l.port2))
            
            self.raiseEvent(MulticastTopoEvent(MulticastTopoEvent.LINK_DOWN, link_changes, self.adjacency))

            # TODO: Check if this is actually neccesary...
            # These routers may still be adjacent through a different link
            for ll in core.openflow_discovery.adjacency:
                if ll.dpid1 == l.dpid1 and ll.dpid2 == l.dpid2 and ll.port1 != l.port1:
                    if flip(ll) in core.openflow_discovery.adjacency:
                        link_changes = []
                        
                        # Yup, link goes both ways
                        log.info('Found parallel adjacency: ' + dpid_to_str(ll.dpid1) + '.'
                                + str(ll.port1) + ' <-> ' + dpid_to_str(ll.dpid2) + '.'
                                + str(ll.port2));
                        self.adjacency[l.dpid1][l.dpid2] = ll.port1
                        link_changes.append((l.dpid1, l.dpid2, ll.port1))
                        if ll.port1 in router1.igmp_ports:
                            router1.igmp_ports.remove(ll.port1)
                        else:
                            log.warn(str(ll.port1) + ' not found in ports of router: ' + dpid_to_str(router1.dpid))
                        self.adjacency[l.dpid2][l.dpid1] = ll.port2
                        link_changes.append((l.dpid2, l.dpid1, ll.port2))
                        if ll.port2 in router2.igmp_ports:
                            router2.igmp_ports.remove(ll.port2)
                        else:
                            log.warn(str(ll.port2) + ' not found in ports of router: ' + dpid_to_str(router2.dpid))
                        # Fixed -- new link chosen to connect these
                        
                        self.raiseEvent(MulticastTopoEvent(MulticastTopoEvent.LINK_UP, link_changes, self.adjacency))
                        break
            
        else:
            link_event = MulticastTopoEvent.LINK_UP
            # If we already consider these nodes connected, we can ignore this link up.
            # Otherwise, we might be interested...
            if self.adjacency[l.dpid1][l.dpid2] is None:
                # These previously weren't connected.  If the link
                # exists in both directions, we consider them connected now.
                if flip(l) in core.openflow_discovery.adjacency:
                    link_changes = []
                    # Link goes both ways -- connected!
                    self.adjacency[l.dpid1][l.dpid2] = l.port1
                    link_changes.append((l.dpid1, l.dpid2, l.port1))
                    if l.port1 in router1.igmp_ports:
                        router1.igmp_ports.remove(l.port1)
                    else:
                        log.warn(str(l.port1) + ' not found in ports of router: ' + dpid_to_str(router1.dpid))
                    self.adjacency[l.dpid2][l.dpid1] = l.port2
                    link_changes.append((l.dpid2, l.dpid1, l.port2))
                    if l.port2 in router2.igmp_ports:
                        router2.igmp_ports.remove(l.port2)
                    else:
                        log.warn(str(l.port2) + ' not found in ports of router: ' + dpid_to_str(router2.dpid))
                    log.info('Added adjacency: ' + str(router1) + '.'
                             + str(l.port1) + ' <-> ' + str(router2) + '.'
                             + str(l.port2))
                             
                    self.raiseEvent(MulticastTopoEvent(MulticastTopoEvent.LINK_UP, link_changes, self.adjacency))
                    
                    

    def _handle_ConnectionUp(self, event):
        """Handler for ConnectionUp from the discovery module, which represents a new router joining the network.
        
        TODO: Investigate whether this should throw MulticastTopoEvents (LinkEvents should really cover the same information)
        """
        self.add_igmp_router(event.dpid, event.connection)
            
    def _handle_ConnectionDown (self, event):
        """Handler for ConnectionDown from the discovery module, which represents a router leaving the network."""
        router = self.routers.get(event.dpid)
        if router is None:
            log.warn('Got ConnectionDown for unrecognized router')
        else:
            log.warn('Router down: ' + dpid_to_str(event.dpid))
            link_changes = []
            # Remove any adjacency information stored for this router
            if event.dpid in self.adjacency:
                for router_dpid in self.adjacency[event.dpid]:
                    link_changes.append((event.dpid, router_dpid, self.adjacency[event.dpid][router_dpid]))
                del self.adjacency[event.dpid]
            for router in self.adjacency:
                if event.dpid in self.adjacency[router]:
                    link_changes.append((event.dpid, router_dpid, self.adjacency[router][event.dpid]))
                    del self.adjacency[router][event.dpid]
            del self.routers[event.dpid]
            event = MulticastTopoEvent(MulticastTopoEvent.LINK_DOWN, link_changes, self.adjacency)
            self.raiseEvent(event)
    
    def _handle_PacketIn(self, event):
        """Handler for OpenFlow PacketIn events. Ignores all packets except IGMPv3 packets.

        Two types of packets are of primary importance for this module:
        
        1. IGMP packets: All IGMP packets in the network should be directed to the controller, as the controller
           acts as a single, centralized IGMPv3 multicast router for the purposes of determining group membership.
        2. IP packets destined to a multicast address: When a new multicast flow is received, this should trigger
           a calculation of a multicast tree, and the installation of the appropriate flows in the network.
        """
        # log.debug('Router ' + str(event.connection.dpid) + ':' + str(event.port) + ' PacketIn')
        
        # Make sure this router is known to the controller, ignore the packet if not
        if not event.connection.dpid in self.routers:
            log.debug('Got packet from unrecognized router: ' + str(event.connection.dpid))
            return
        router_dpid = event.connection.dpid
        receiving_router = self.routers[router_dpid]
        
        igmp_pkt = event.parsed.find(pkt.igmpv3)
        if not igmp_pkt is None:
            # ==== IGMP packet - IPv4 Network ====
            # Determine the source IP of the packet
            log.debug(str(receiving_router) + ':' + str(event.port) + '| Received IGMP packet')
            ipv4_pkt = event.parsed.find(pkt.ipv4)
            log.debug(str(receiving_router) + ':' + str(event.port) + '| ' + str(igmp_pkt) + ' from Host: ' + str(ipv4_pkt.srcip))
            
            # Check to see if this IGMP message was received from a neighbouring router, and if so
            # add a rule to drop additional IGMP packets on this port
            for neighbour in self.adjacency[router_dpid]:
                if self.adjacency[router_dpid][neighbour] == event.port:
                    log.debug(str(receiving_router) + ':' + str(event.port) + '| IGMP packet received from neighbouring router.')
                    
                    # TODO: This doesn't appear to actually be working with mininet, just drop individual IGMP packets without installing a flow for now
                    # msg = of.ofp_flow_mod()
                    # msg.match.dl_type = ipv4_pkt.protocol
                    # msg.match.nw_dst = ipv4_pkt.dstip
                    # msg.match.in_port = event.port
                    # msg.action = []     # No actions = drop packet
                    # event.connection.send(msg)
                    # log.info(str(receiving_router) + ':' + str(event.port) + '| Installed flow to drop all IGMP packets on port: ' + str(event.port))
                    
                    self.drop_packet(event)
                    return
            
            # Create a new trace event for benchmarking purposes
            igmp_trace_event = None
            try:
                igmp_trace_event = core.groupflow_event_tracer.init_igmp_event_trace(router_dpid)
            except:
                pass
            
            # Have the receiving router process the IGMP packet accordingly
            receiving_router.process_igmp_event(event, igmp_trace_event)
            
            # Drop the IGMP packet to prevent it from being uneccesarily forwarded to neighbouring routers
            self.drop_packet(event)
            return


def launch():
    # Method called by the POX core when launching the module
    core.registerNew(IGMPManager)