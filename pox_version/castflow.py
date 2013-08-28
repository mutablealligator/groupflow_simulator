#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
A POX module implementation of the CastFlow clean slate multicast proposal.

Implementation adapted from NOX-Classic CastFlow implementation provided by caioviel.

Depends on openflow.discovery

WARNING: This module is not complete, and should currently only be tested on loop free topologies

Created on July 16, 2013
@author: alexcraig
'''

from collections import defaultdict
from heapq import heapify, heappop, heappush

# POX dependencies
from pox.openflow.discovery import Discovery
from pox.core import core
from pox.lib.revent import *
from pox.lib.util import dpid_to_str
import pox.lib.packet as pkt
from pox.lib.packet.igmp import *   # Required for various IGMP variable constants
from pox.lib.packet.ethernet import *
import pox.openflow.libopenflow_01 as of
from pox.lib.addresses import IPAddr, EthAddr
from pox.lib.recoco import Timer
import time

log = core.getLogger()

def int_to_filter_mode_str(filter_mode):
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

class MulticastMembershipRecord:
    """Class representing the groud record state maintained by an IGMPv3 multicast router

    Multicast routers implementing IGMPv3 keep state per group per
    attached network.  This group state consists of a filter-mode, a list
    of sources, and various timers.  For each attached network running
    IGMP, a multicast router records the desired reception state for that
    network.  That state conceptually consists of a set of records of the
    form:

        (multicast address, group timer, filter-mode, (source records))

    Each source record is of the form:

        (source address, source timer)
    """
    
    def __init__(self, mcast_address, timer_value):
        self.multicast_address = mcast_address
        self.group_timer = timer_value
        self.filter_mode = MODE_IS_INCLUDE  # TODO: Re-examine this as the default
        self.x_source_records = [] # Source records are stored as a list of the source address and the source timer
        self.y_source_records = [] # Source records with a zero source timer are stored separately
    
    def get_curr_source_timer(self, ip_addr):
        for record in self.x_source_records:
            if record[0] == ip_addr:
                return record[1]
         
        return 0
        
    def get_x_addr_set(self):
        return_set = set()
        for record in self.x_source_records:
            return_set.add(record[0])
        return return_set
    
    def get_y_addr_set(self):
        return_set = set()
        for record in self.y_source_records:
            return_set.add(record[0])
        return return_set
    
    def remove_source_record(self, ip_addr):
        for record in self.x_source_records:
            if record[0] == ip_addr:
                self.x_source_records.remove(record)
                return
        
        for record in self.y_source_records:
            if record[0] == ip_addr:
                self.y_source_records.remove(record)
                return
    
    def addr_in_x_source_records(self, ip_addr):
        for record in self.x_source_records:
            if record[0] == ip_addr:
                return True
                
        return False
        
        
    def addr_in_y_source_records(self, ip_addr):
        for record in self.y_source_records:
            if record[0] == ip_addr:
                return True
        
        return False
        

class Router(EventMixin):
    """
    Class representing an OpenFlow router controlled by the CastFlow manager
    """

    def __init__(self, manager):
        self.connection = None
        self.ports = None # Complete list of all ports on the router, contains complete port object provided by connection objects
        self.igmp_ports = [] # List of ports over which IGMP service should be provided, contains only integer indexes
        
        # self.multicast_records[igmp_port_index][multicast_address]
        self.multicast_records = defaultdict(lambda : defaultdict(lambda : None))
        self.dpid = None
        self.castflow_manager = manager
        self._listeners = None
        self._connected_at = None
        
        # Dictionary of connected host IPs keyed by port numbers
        # TODO: Determine if this is actually neccesary (probably not, as the router shouldn't need to know which hosts
        # behind each interface are actually requesting multicast traffic)
        # self.connected_hosts = defaultdict(lambda : None)

    def __repr__(self):
        return dpid_to_str(self.dpid)

    def disconnect(self):
        if self.connection is not None:
            log.debug('Disconnect %s' % (self.connection, ))
            self.connection.removeListeners(self._listeners)
            self.connection = None
            self._listeners = None

    def connect(self, connection):
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
        self.disconnect()
        log.debug('Connect %s' % (connection, ))
        self.connection = connection
        self._listeners = self.listenTo(connection)
        self._connected_at = time.time()

    @property
    def is_holding_down(self):
        if self._connected_at is None:
            return True
        if time.time() - self._connected_at > FLOOD_HOLDDOWN:
            return False
        return True

    def _handle_ConnectionDown(self, event):
        self.disconnect()
        
    def print_group_records(self):
        log.debug(' ')
        log.debug('BEGIN == Router ' + str(self) + ' Group Membership State ==')
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
        log.debug('END =====================================================')
        log.debug(' ')
    
    def create_group_record(self, event, igmp_group_record, group_timer):
        if self.multicast_records[event.port][igmp_group_record.multicast_address] is None:
            self.multicast_records[event.port][igmp_group_record.multicast_address] = \
                    MulticastMembershipRecord(igmp_group_record.multicast_address, group_timer)
            log.debug('Added group record for multicast IP: ' + str(igmp_group_record.multicast_address))
        
        return self.multicast_records[event.port][igmp_group_record.multicast_address]
    
    def send_group_specific_query(self, port, multicast_address, retransmissions = -1):
        return  # Debug - Not yet implemented
        
        if retransmissions == -1:
            self.castflow_manager.igmp_robustness - 1
            
        # Build the IGMP payload for the message
        igmp_pkt = pkt.igmp()
        igmp_pkt.ver_and_type = MEMBERSHIP_QUERY
        igmp_pkt.max_response_time = self.castflow_manager.igmp_query_response_interval
        igmp_pkt.address = multicast_address
        igmp_pkt.qrv = self.castflow_manager.igmp_robustness
        igmp_pkt.qqic = self.castflow_manager.igmp_query_interval
        igmp_pkt.dlen = 12  # TODO: This should be determined by the IGMP packet class somehow
        eth_pkt = self.castflow_manager.encapsulate_igmp_packet(igmp_pkt)
        output = of.ofp_packet_out(action = of.ofp_action_output(port=port))
        output.data = eth_pkt.pack()
        output.pack()
        self.connection.send(output)
        log.info('Router ' + str(self) + ':' + str(port) + '| Sent group specific query for group: ' + str(multicast_address))
        self.multicast_records[port][multicast_address].group_timer = self.igmp_last_member_query_time / 10
        
        if retransmissions > 0:
            Timer(self.igmp_last_member_query_interval, send_group_specific_query, 
                    args = [self, port, multicast_address, retransmissions - 1], recurring = False)
                    
    def send_group_and_source_specific_query(self, port, multicast_address, sources, retransmissions = -1):
        return  # Debug - Not yet implemented
        
        if retransmissions == -1:
            self.castflow_manager.igmp_robustness - 1
        # Build the IGMP payload for the message
        igmp_pkt = pkt.igmp()
        igmp_pkt.ver_and_type = MEMBERSHIP_QUERY
        igmp_pkt.max_response_time = self.castflow_manager.igmp_query_response_interval
        igmp_pkt.address = multicast_address
        igmp_pkt.qrv = self.castflow_manager.igmp_robustness
        igmp_pkt.qqic = self.castflow_manager.igmp_query_interval
        igmp_pkt.dlen = 12  # TODO: This should be determined by the IGMP packet class somehow
        igmp_pkt.num_sources = len(sources)
        for source in sources:
            igmp_pkt.source_addresses.append(source[0])
            
        igmp_group_record = igmpv3_group_record()

        eth_pkt = self.castflow_manager.encapsulate_igmp_packet(igmp_pkt)
        output = of.ofp_packet_out(action = of.ofp_action_output(port=port))
        output.data = eth_pkt.pack()
        output.pack()
        self.connection.send(output)
        log.info('Router ' + str(self) + ':' + str(port) + '| Sent group specific query for group: ' + str(multicast_address))
        for source in sources:
            log.info('Source: ' + str(source[0]))
        self.multicast_records[port][multicast_address].group_timer = self.igmp_last_member_query_time / 10
        
        if retransmissions > 0:
            Timer(self.igmp_last_member_query_interval, send_group_specific_query, 
                    args = [self, port, multicast_address, retransmissions - 1], recurring = False)
        
    
    def remove_group_record(self, port, multicast_address):
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
        """Processes state change IGMP membership reports according to the following table:
        
        Router State   Report Rec'd New Router State        Actions
        ------------   ------------ ----------------        -------

        INCLUDE (A)    ALLOW (B)    INCLUDE (A+B)           (B)=GMI

        INCLUDE (A)    BLOCK (B)    INCLUDE (A)             Send Q(G,A*B)

        INCLUDE (A)    TO_EX (B)    EXCLUDE (A*B,B-A)       (B-A)=0
                                                            Delete (A-B)
                                                            Send Q(G,A*B)
                                                            Group Timer=GMI

        INCLUDE (A)    TO_IN (B)    INCLUDE (A+B)           (B)=GMI
                                                            Send Q(G,A-B)

        EXCLUDE (X,Y)  ALLOW (A)    EXCLUDE (X+A,Y-A)       (A)=GMI

        EXCLUDE (X,Y)  BLOCK (A)    EXCLUDE (X+(A-Y),Y)     (A-X-Y)=Group Timer
                                                            Send Q(G,A-Y)

        EXCLUDE (X,Y)  TO_EX (A)    EXCLUDE (A-Y,Y*A)       (A-X-Y)=Group Timer
                                                            Delete (X-A)
                                                            Delete (Y-A)
                                                            Send Q(G,A-Y)
                                                            Group Timer=GMI
                                                            
        EXCLUDE (X,Y)  TO_IN (A)    EXCLUDE (X+A,Y-A)       (A)=GMI
                                                            Send Q(G,X-A)
                                                            Send Q(G)
        
        Note: When the group is in INCLUDE mode, the set of addresses is stored in
        the same list as the X set when in EXCLUDE mode
        """
        router_group_record = self.create_group_record(event, igmp_group_record,
                    self.castflow_manager.igmp_group_membership_interval)
        new_x_source_records = []
        new_y_source_records = []
        
        if router_group_record.filter_mode == MODE_IS_INCLUDE:
            if igmp_group_record.record_type == ALLOW_NEW_SOURCES:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received ALLOW_NEW_SOURCES')
                new_x_source_records = router_group_record.x_source_records[:]
                for source_address in igmp_group_record.source_addresses:
                    record_already_present = False
                    for source_record in new_x_source_records:
                        if source_record[0] == source_address:
                            source_record[1] = self.castflow_manager.igmp_group_membership_interval
                            record_already_present = True
                            break
                    if not record_already_present:
                        new_x_source_records.append([source_address, 
                            self.castflow_manager.igmp_group_membership_interval])
            
            elif igmp_group_record.record_type == BLOCK_OLD_SOURCES:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received BLOCK_OLD_SOURCES')
                query_sources = []
                for source_address in igmp_group_record.source_addresses:
                    if router_group_record.addr_in_x_source_records(source_address):
                        sources.append([source_address])
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, sources)
                
            elif igmp_group_record.record_type == CHANGE_TO_EXCLUDE_MODE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received CHANGE_TO_EXCLUDE_MODE')
                router_group_record.filter_mode = MODE_IS_EXCLUDE
                new_y_source_records = router_group_record.x_source_records[:]
                for source_address in igmp_group_record.source_addresses:
                    if router_group_record.addr_in_x_source_records(source_address):
                        new_x_source_records.append([source_address, 
                                router_group_record.get_curr_source_timer(source_address)])
                        new_y_to_remove = None
                        for new_y_record in new_y_source_records:
                            if new_y_record[0] == source_address:
                                new_y_to_remove = new_y_record
                                break
                        if new_y_to_remove is not None:
                            new_y_source_records.remove(new_y_to_remove)
                            
                for new_y_record in new_y_source_records:
                    new_y_record[1] = 0
                
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, new_x_source_records)
                
                router_group_record.group_timer = self.castflow_manager.igmp_group_membership_interval
                
            elif igmp_group_record.record_type == CHANGE_TO_INCLUDE_MODE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received CHANGE_TO_INCLUDE_MODE')
                new_x_source_records = router_group_record.x_source_records[:]
                # Add any new sources which did not already appear with a
                # refreshed timer
                # X = X + B
                for source_address in igmp_group_record.source_addresses:
                    record_already_present = False
                    for source_record in new_x_source_records:
                        if source_record[0] == source_address:
                            source_record[1] = self.castflow_manager.igmp_group_membership_interval
                            record_already_present = True
                            break
                    if not record_already_present:
                        new_x_source_records.append([source_address, 
                            self.castflow_manager.igmp_group_membership_interval])
                            
                # TODO: Send Q(G,A*B)

        elif router_group_record.filter_mode == MODE_IS_EXCLUDE:
            if igmp_group_record.record_type == ALLOW_NEW_SOURCES:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received ALLOW_NEW_SOURCES')
                new_x_source_records = router_group_record.x_source_records[:]
                new_y_source_records = router_group_record.x_source_records[:]
                
                for source_address in igmp_group_record.source_addresses:
                    # Update X * A with new GMI
                    record_already_present = False
                    for source_record in new_x_source_records:
                        if source_record[0] == source_address:
                            source_record[1] = self.castflow_manager.igmp_group_membership_interval
                            record_already_present = True
                            break
                    
                    # X = X + A
                    if not record_already_present:
                        new_x_source_records.append([source_address, 
                            self.castflow_manager.igmp_group_membership_interval])
                    
                    # Y = Y - A
                    source_record_to_remove = None                    
                    for source_record in new_y_source_records:
                        if source_record[0] == source_address:
                            source_record_to_remove = source_record
                            break
                    if source_record_to_remove is not None:
                        new_y_source_records.remove(source_record)
                
            elif igmp_group_record.record_type == BLOCK_OLD_SOURCES:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received BLOCK_OLD_SOURCES')
                new_x_source_records = router_group_record.x_source_records[:]
                new_y_source_records = router_group_record.y_source_records[:]
                
                for source_address in igmp_group_record.source_addresses:
                    if not router_group_record.addr_in_x_source_records(source_address) and \
                            not router_group_record.addr_in_y_source_records(source_address):
                        new_x_source_records.append([source_address, 
                                router_group_record.group_timer])
                        continue
                
                # TODO: Send Q(G, A-Y)
                    
            elif igmp_group_record.record_type == CHANGE_TO_EXCLUDE_MODE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received CHANGE_TO_EXCLUDE_MODE')
                new_x_source_records = []
                new_y_source_records = []
                
                for source_address in igmp_group_record.source_addresses:
                    if router_group_record.addr_in_y_source_records(source_address):
                        new_y_source_records.append([source_address, 0])
                        continue
                        
                    if not router_group_record.addr_in_x_source_records(source_address) and \
                            not router_group_record.addr_in_y_source_records(source_address):
                        new_x_source_records.append([source_address, 
                                router_group_record.group_timer])
                        continue
                    
                    if not router_group_record.addr_in_y_source_records(source_address):
                        new_x_source_records.append([source_address, 
                                router_group_record.get_curr_source_timer(source_address)])
                        continue
                
                self.send_group_and_source_specific_query(event.port, igmp_group_record.multicast_address, new_x_source_records)
                
                router_group_record.group_timer = self.castflow_manager.igmp_group_membership_interval
                    
            elif igmp_group_record.record_type == CHANGE_TO_INCLUDE_MODE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received CHANGE_TO_INCLUDE_MODE')
                new_x_source_records = router_group_record.x_source_records[:]
                new_y_source_records = router_group_record.x_source_records[:]
                
                for source_address in igmp_group_record.source_addresses:
                    # Update X * A with new GMI
                    record_already_present = False
                    for source_record in new_x_source_records:
                        if source_record[0] == source_address:
                            source_record[1] = self.castflow_manager.igmp_group_membership_interval
                            record_already_present = True
                            break
                    
                    # X = X + A
                    if not record_already_present:
                        new_x_source_records.append([source_address, 
                            self.castflow_manager.igmp_group_membership_interval])
                    
                    # Y = Y - A
                    source_record_to_remove = None                    
                    for source_record in new_y_source_records:
                        if source_record[0] == source_address:
                            source_record_to_remove = source_record
                            break
                    if source_record_to_remove is not None:
                        new_y_source_records.remove(source_record)
                        
                    # TODO: Send Q(G, X-A)
                    
                    self.send_group_specific_query(event.port, igmp_group_record.multicast_address)
                    
                    
        if router_group_record.filter_mode == MODE_IS_INCLUDE and len(new_x_source_records) == 0:
            self.remove_group_record(event.port, igmp_group_record.multicast_address)
        else:
            router_group_record.x_source_records = new_x_source_records
            router_group_record.y_source_records = new_y_source_records
    
    def process_current_state_record(self, event, igmp_group_record):
        """Processes current state IGMP membership reports according to the following table:
          
        Router State   Report Rec'd  New Router State         Actions
        ------------   ------------  ----------------         -------
        
        INCLUDE (A)    IS_IN (B)     INCLUDE (A+B)            (B)=GMI
        
        INCLUDE (A)    IS_EX (B)     EXCLUDE (A*B,B-A)        (B-A)=0
                                                              Delete (A-B)
                                                              Group Timer=GMI

        EXCLUDE (X,Y)  IS_IN (A)     EXCLUDE (X+A,Y-A)        (A)=GMI

        EXCLUDE (X,Y)  IS_EX (A)     EXCLUDE (A-Y,Y*A)        (A-X-Y)=GMI
                                                              Delete (X-A)
                                                              Delete (Y-A)
                                                              Group Timer=GMI
                                                              
        Note: When the group is in INCLUDE mode, the set of addresses is stored in
        the same list as the X set when in EXCLUDE mode
        """
        
        router_group_record = self.create_group_record(event, igmp_group_record,
                    self.castflow_manager.igmp_group_membership_interval)
        new_x_source_records = []
        new_y_source_records = []
        
        igmp_record_addresses = igmp_group_record.get_addr_set()
        
        if router_group_record.filter_mode == MODE_IS_INCLUDE:
            if igmp_group_record.record_type == MODE_IS_INCLUDE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received MODE_IS_INCLUDE')
                
                new_x_set = router_group_record.get_x_addr_set() + igmp_record_addresses
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.castflow_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                
            elif igmp_group_record.record_type == MODE_IS_EXCLUDE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is INCLUDE, Received MODE_IS_EXCLUDE')
                router_group_record.filter_mode = MODE_IS_EXCLUDE
                
                new_x_set = router_group_record.get_x_addr_set().intersection(igmp_record_addresses)
                new_y_set = igmp_record_addresses - router_group_record.get_x_addr_set()
                for address in new_x_set:
                    new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                router_group_record.group_timer = self.castflow_manager.igmp_group_membership_interval
                
        elif router_group_record.filter_mode == MODE_IS_EXCLUDE:
            if igmp_group_record.record_type == MODE_IS_INCLUDE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received MODE_IS_INCLUDE')
                
                new_x_set = router_group_record.get_x_addr_set() + igmp_record_addresses
                new_y_set = router_group_record.get_y_addr_set().intersection(igmp_record_addresses)
                gmi_set = (igmp_record_addresses - router_group_record.get_x_addr_set()) - router_group_record.get_y_addr_set()
                
                for address in new_x_set:
                    if address in igmp_record_addresses:
                        new_x_source_records.append([address, self.castflow_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                    
                            
            elif igmp_group_record.record_type == MODE_IS_EXCLUDE:
                log.debug(str(self) + ':' + str(event.port) + '|' + str(igmp_group_record.multicast_address) + ' is EXCLUDE, Received MODE_IS_EXCLUDE')
                
                new_x_set = igmp_record_addresses - router_group_record.get_y_addr_set()
                new_y_set = router_group_record.get_y_addr_set().intersect(igmp_record_addresses)
                gmi_set = (igmp_record_addresses - router_group_record.get_x_addr_set()) - router_group_record.get_y_addr_set()
                
                for address in new_x_set:
                    if address in gmi_set:
                        new_x_source_records.append([address, self.castflow_manager.igmp_group_membership_interval])
                    else:
                        new_x_source_records.append([address, router_group_record.get_curr_source_timer(address)])
                
                for address in new_y_set:
                    new_y_source_records.append([address, 0])
                
                router_group_record.group_timer = self.castflow_manager.igmp_group_membership_interval

        # Prune any INCLUDE_MODE records which do not specify any sources
        if router_group_record.filter_mode == MODE_IS_INCLUDE and len(new_x_source_records) == 0:
            self.remove_group_record(event.port, igmp_group_record.multicast_address)
        else:
            router_group_record.x_source_records = new_x_source_records
            router_group_record.y_source_records = new_y_source_records
    
    def process_igmp_event(self, event):
        """Processes any IGMP event received by the router."""
        
        igmp_pkt = event.parsed.find(pkt.igmp)
        
        if igmp_pkt.msg_type != MEMBERSHIP_REPORT_V3:
            return
            
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
            # each group record is processed
            self.print_group_records()
        
        


class CastflowManager(object):

    def __init__(self):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self)
            core.openflow_discovery.addListeners(self)
        
        # Set variables for IGMP operation to default values
        self.igmp_robustness = 2
        self.igmp_query_interval = 125               # Seconds
        # Debug: Make the IGMP query interval shorter than default to aid testing
        self.igmp_query_interval = 20
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
        # Adjacency map:  [router1][router2] -> port from router1 to router2
        self.adjacency = defaultdict(lambda : defaultdict(lambda : \
                None))

        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'))
    
    def decrement_all_timers(self):
        """Decrements the source and group timers for all group_records in the network. As long as this is
        always called from a recoco Timer, mutual exclusion should not be an issue.
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
                router.print_group_records()
        
    def encapsulate_igmp_packet(self, igmp_pkt):
        """Encapsulates the provided IGMP packet into IP and Ethernet packets, and returns the
        encapsulating ethernet packet"""
        
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
        """Encapsulates the provided IGMP packet into IP and Ethernet packets, and sends the packet
        to all attached network on all routers."""
        # Build the encapsulating IP/ethernet packet
        eth_pkt = self.encapsulate_igmp_packet(igmp_pkt)
        
        # Send out the packet
        for router_dpid in self.routers:
            sending_router = self.routers[router_dpid]
            # for neighbour in self.adjacency[sending_router]:  # Debug, send to neighbouring routers only
            #    port_num = self.adjacency[sending_router][neighbour]
            for port_num in sending_router.igmp_ports:
                output = of.ofp_packet_out(action = of.ofp_action_output(port=port_num))
                output.data = eth_pkt.pack()
                output.pack()
                sending_router.connection.send(output)
                # log.debug('Router ' + str(sending_router) + ' sending IGMP query on port: ' + str(port_num))
        
    def launch_igmp_general_query(self):
        """Generates an IGMP general query, broadcasts it from all ports on all routers
        
        TODO: This could be improved by only broadcasting the query on ports which are not
        connected to an adjacent router in the same control domain, as these queries will
        be discarded by the adjacent routers anyway.
        """
        log.debug('Launching IGMP general query from all routers')
        
        # Build the IGMP payload for the message
        igmp_pkt = pkt.igmp()
        igmp_pkt.ver_and_type = MEMBERSHIP_QUERY
        igmp_pkt.max_response_time = self.igmp_query_response_interval
        igmp_pkt.address = IPAddr("0.0.0.0")
        igmp_pkt.qrv = self.igmp_robustness
        igmp_pkt.qqic = self.igmp_query_interval
        igmp_pkt.dlen = 12  # TODO: This should be determined by the IGMP packet class somehow
        
        self.send_igmp_query_to_all_networks(igmp_pkt)
        
        
    def drop_packet(self, packet_in_event):
        """Drops the packet represented by the PacketInEvent without any flow table modification"""
        msg = of.ofp_packet_out()
        msg.data = packet_in_event.ofp
        msg.buffer_id = packet_in_event.ofp.buffer_id
        msg.in_port = packet_in_event.port
        msg.actions = []    # No actions = drop packet
        packet_in_event.connection.send(msg)
        # log.debug('Packet dropped')
        

    def _handle_LinkEvent(self, event):
        """Handler for LinkEvents from the discovery module, which are used to learn the network topology."""

        def flip(link):
            return Discovery.Link(link[2], link[3], link[0], link[1])

        # log.info('Handling LinkEvent: ' + str(event.link)
        #          + ' - State: ' + str(event.added))
        # if event.added:
        #    self.links.append(event.link)
        # else:
        #    self.links.remove(event.link)
        # log.debug(self.links)

        l = event.link
        router1 = self.routers[l.dpid1]
        router2 = self.routers[l.dpid2]

        if event.removed:
            # This link no longer up
            if router2 in self.adjacency[router1]:
                del self.adjacency[router1][router2]
            if router1 in self.adjacency[router2]:
                del self.adjacency[router2][router1]
            router1.igmp_ports.append(l.port1)
            router2.igmp_ports.append(l.port2)
                
            log.info('Removed adjacency: ' + str(router1) + '.'
                 + str(l.port1) + ' <-> ' + str(router2) + '.'
                 + str(l.port2))

            # These routers may still be adjacent through a different link
            for ll in core.openflow_discovery.adjacency:
                if ll.dpid1 == l.dpid1 and ll.dpid2 == l.dpid2:
                    if flip(ll) in core.openflow_discovery.adjacency:
                        # Yup, link goes both ways
                        log.info('Found parallel adjacency');
                        self.adjacency[router1][router2] = ll.port1
                        router1.igmp_ports.remove(ll.port1)
                        self.adjacency[router2][router1] = ll.port2
                        router2.igmp_ports.remove(ll.port2)
                        # Fixed -- new link chosen to connect these
                        break
        else:
            # If we already consider these nodes connected, we can ignore this link up.
            # Otherwise, we might be interested...
            if self.adjacency[router1][router2] is None:
                # These previously weren't connected.  If the link
                # exists in both directions, we consider them connected now.
                if flip(l) in core.openflow_discovery.adjacency:
                    # Link goes both ways -- connected!
                    self.adjacency[router1][router2] = l.port1
                    router1.igmp_ports.remove(l.port1)
                    self.adjacency[router2][router1] = l.port2
                    router2.igmp_ports.remove(l.port2)
                    log.info('Added adjacency: ' + str(router1) + '.'
                             + str(l.port1) + ' <-> ' + str(router2) + '.'
                             + str(l.port2))

                    # Remove router IPs that have been previously identified as hosts
                    #if l.port1 in router1.connected_hosts:
                    #    log.debug('Deleted connected host (' + str(router1.connected_hosts[l.port1]) + ') on port ' + str(l.port1) + ' (host is actually a router)')
                    #    del router1.connected_hosts[l.port1]
                    #if l.port2 in router2.connected_hosts:
                    #    log.debug('Deleted connected host (' + str(router2.connected_hosts[l.port2]) + ') on port ' + str(l.port2) + ' (host is actually a router)')
                    #    del router2.connected_hosts[l.port2]

                    # While it is tempting to add a flow table entry to block IGMP messages from being transmitted between
                    # adjoining routers, this would also cause IGMP packets to be dropped silently without processing
                    # by the controller.

    def _handle_ConnectionUp(self, event):
        """Handler for ConnectionUp from the discovery module, which represent new routers joining the network."""
        router = self.routers.get(event.dpid)
        if router is None:
            # New router
            router = Router(self)
            router.dpid = event.dpid
            self.routers[event.dpid] = router
            log.info('Learned new router: ' + str(router))
            router.connect(event.connection)
        
        if not self.got_first_connection_up:
            self.got_first_connection_up = True
            # Setup the Timer to send periodic general queries
            self.general_query_timer = Timer(self.igmp_query_interval, self.launch_igmp_general_query, recurring = True)
            log.debug('Launching IGMP general query timer with interval ' + str(self.igmp_query_interval) + ' seconds')
            # Setup the timer to handle group and source timers
            self.timer_decrementing_timer = Timer(1, self.decrement_all_timers, recurring = True)
            
    def _handle_ConnectionDown (self, event):
        """Handler for ConnectionUp from the discovery module, which represent a router leaving the network."""
        router = self.routers.get(event.dpid)
        if router is None:
            log.warn('Got ConnectionDown for unrecognized router')
        else:
            log.info('Router down: ' + str(self.routers[event.dpid]))
            del self.routers[event.dpid]
    
    def _handle_PacketIn(self, event):
        """Handler for OpenFlow PacketIn events.

        Two types of packets are of primary importance for this module:
        1) IGMP packets: All IGMP packets in the network should be directed to the controller, as the controller
        acts as a single, centralized IGMPv3 multicast router for the purposes of determining group membership.
        2) IP packets destined to a multicast address: When a new multicast flow is received, this should trigger
        a calculation of a multicast tree, and the installation of the appropriate flows in the network.
        """
        # log.debug('Router ' + str(event.connection.dpid) + ':' + str(event.port) + ' PacketIn')
        
        # Make sure this router is known to the controller, ignore the packet if not
        if not event.connection.dpid in self.routers:
            log.debug('Got packet from unrecognized router: ' + str(event.connection.dpid))
            return
        receiving_router = self.routers[event.connection.dpid]
        
        igmp_pkt = event.parsed.find(pkt.igmp)
        if not igmp_pkt is None:
            # ==== IGMP packet - IPv4 Network ====
            # Determine the source IP of the packet
            log.debug(str(receiving_router) + ':' + str(event.port) + '| Received IGMP packet')
            ipv4_pkt = event.parsed.find(pkt.ipv4)
            log.debug(str(receiving_router) + ':' + str(event.port) + '| ' + str(igmp_pkt) + ' from Host: ' + str(ipv4_pkt.srcip))
            
            # Check to see if this IGMP message was received from a neighbouring router, and if so
            # add a rule to drop additional IGMP packets on this port
            for neighbour in self.adjacency[receiving_router]:
                if self.adjacency[receiving_router][neighbour] == event.port:
                    log.debug(str(receiving_router) + ':' + str(event.port) + '| IGMP packet received from neighbouring router.')
                    
                    # TODO: This doesn't appear to actually be working with mininet, just drop individual IGMP packets without installing a flow for now
                    # msg = of.ofp_flow_mod()
                    # msg.match.dl_type = ipv4_pkt.protocol
                    # msg.match.nw_dst = ipv4_pkt.dstip
                    # msg.match.in_port = event.port
                    ## msg.actions.append(of.ofp_action_output(port = of.OFPP_NONE))
                    # msg.action = []     # No actions = drop packet
                    # event.connection.send(msg)
                    # log.info(str(receiving_router) + ':' + str(event.port) + '| Installed flow to drop all IGMP packets on port: ' + str(event.port))
                    
                    self.drop_packet(event)
                    return
            
            # The host must be directly connected to this router (or it is a router in an adjoining
            # network outside the controllers administrative domain), learn its IP and port
            # if not event.port in receiving_router.connected_hosts:
            #    receiving_router.connected_hosts[event.port] = ipv4_pkt.srcip
            #    log.info('Learned new host (IGMP packet): ' + str(ipv4_pkt.srcip) + ':' + str(event.port))
            
            # Have the receiving router process the IGMP packet accordingly
            receiving_router.process_igmp_event(event)
            
            # Drop the IGMP packet to prevent it from being uneccesarily forwarded to neighbouring routers
            self.drop_packet(event)
            return
            
        ipv4_pkt = event.parsed.find(pkt.ipv4)
        if not ipv4_pkt is None:
            # ==== IPv4 Packet ====
            # Check the destination address to see if this is a multicast packet
            if ipv4_pkt.dstip.inNetwork('224.0.0.0/4'):
                log.debug(str(receiving_router) + ':' + str(event.port) + '| Received non-IGMP multicast packet')
            
                from_neighbour_router = False
                for neighbour in self.adjacency[receiving_router]:
                    if self.adjacency[receiving_router][neighbour] == event.port:
                        from_neighbour_router = True
                        break
                
                #if not from_neighbour_router:
                    # This packet must be from a connected host (more specifically, a multicast sender)
                    # if not event.port in receiving_router.connected_hosts:
                    #    receiving_router.connected_hosts[event.port] = ipv4_pkt.srcip
                    #    log.info('Learned new host (multicast packet): ' + str(ipv4_pkt.srcip) + ':' + str(event.port))
                
                # For now, just flood all packets from this multicast group
                # TODO: This will completely break in the presence of loops, make sure that testing uses loop free
                #       topologies until the actual multicast protocol is in place
                
                # TODO: Does a separate ofp_packet_out msg actually need to be send to forward this particular packet,
                #       or is the flow mod sufficient?
                msg = of.ofp_packet_out()
                msg.data = event.ofp
                msg.buffer_id = event.ofp.buffer_id
                msg.in_port = event.port
                msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
                event.connection.send(msg)
        
                msg = of.ofp_flow_mod()
                msg.match.dl_type = 0x800   # IPV4
                msg.match.nw_dst = ipv4_pkt.dstip
                msg.match.in_port = event.port
                msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
                event.connection.send(msg)
                log.info(str(receiving_router) + ':' + str(event.port) + '| Installed flow to flood all packets for multicast group: ' + str(ipv4_pkt.dstip))
                
        
        
def launch():
    core.registerNew(CastflowManager)


