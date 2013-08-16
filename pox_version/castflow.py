#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
A POX module implementation of the CastFlow clean slate multicast proposal.

Implementation adapted from NOX-Classic CastFlow implementation provided by caioviel.

Depends on openflow.discovery

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
import pox.openflow.libopenflow_01 as of
from pox.lib.addresses import IPAddr, EthAddr

log = core.getLogger()

class MulticastMembershipRecord:
    '''Multicast routers implementing IGMPv3 keep state per group per
    attached network.  This group state consists of a filter-mode, a list
    of sources, and various timers.  For each attached network running
    IGMP, a multicast router records the desired reception state for that
    network.  That state conceptually consists of a set of records of the
    form:

        (multicast address, group timer, filter-mode, (source records))

    Each source record is of the form:

        (source address, source timer)
    '''
    def __init__(self):
        self.multicast_address = None
        self.group_timer = None
        self.filter_mode = MODE_IS_INCLUDE  # TODO - Re-examine this as the default
        source_records = [] # Source records are stored as a tuple of the source address and the source timer

        
class Router(EventMixin):

    def __init__(self):
        self.connection = None
        self.ports = None
        self.dpid = None
        self._listeners = None
        self._connected_at = None
        
        # Dictionary of connected host IPs keyed by port numbers
        self.connected_hosts = defaultdict(lambda : None)

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


class CastflowManager(object):

    def __init__(self):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self)
            core.openflow_discovery.addListeners(self)
        
        # Set variables for IGMP operation to default values
        self.igmp_robustness = 2
        self.igmp_query_inteval = 125               # Seconds
        self.igmp_query_response_interval = 100     # Tenths of a second
        self.igmp_group_membership_interval = (self.igmp_robustness * self.igmp_query_inteval) \
                + (self.igmp_query_response_interval * 0.1)             # Seconds
        # Note: Other querier present interval should not actually be required with this centralized
        # implementation
        self.igmp_other_querier_present_interval = (self.igmp_robustness * self.igmp_query_inteval) \
                + ((self.igmp_query_response_interval * 0.1) / 2)       # Seconds
        self.igmp_startup_query_interval = self.igmp_query_inteval / 4  # Seconds
        self.igmp_startup_query_count = self.igmp_robustness
        self.igmp_last_member_query_interval = 10                       # Tenths of a second
        self.igmp_last_member_query_count = self.igmp_robustness
        self.igmp_last_member_query_time = self.igmp_last_member_query_interval \
                * self.igmp_last_member_query_count                     # Tenths of a second
        self.igmp_unsolicited_report_interval = 1       # Seconds (not used in router implementation)

        # Setup topology discovery state
        
        # Known routers:  [dpid] -> Router
        self.routers = {}
        # Adjacency map:  [router1][router2] -> port from router1 to router2
        self.adjacency = defaultdict(lambda : defaultdict(lambda : \
                None))
        
        # Setup IGMP state
        self.membership_records = []
                
                
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'))
        
    def drop_packet(self, packet_in_event):
        msg = of.ofp_packet_out()
        msg.data = packet_in_event.ofp
        msg.buffer_id = packet_in_event.ofp.buffer_id
        msg.in_port = packet_in_event.port
        msg.actions = []    # No actions = drop packet
        packet_in_event.connection.send(msg)
        log.debug('Packet dropped')

    def _handle_LinkEvent(self, event):

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
            # This link no longer okay
            if router2 in self.adjacency[router1]:
                del self.adjacency[router1][router2]
            if router1 in self.adjacency[router2]:
                del self.adjacency[router2][router1]
                
            log.info('Removed adjacency: ' + str(router1) + '.'
                 + str(l.port1) + ' <-> ' + str(router2) + '.'
                 + str(l.port2))

            # But maybe there's another way to connect these...
            for ll in core.openflow_discovery.adjacency:
                if ll.dpid1 == l.dpid1 and ll.dpid2 == l.dpid2:
                    if flip(ll) in core.openflow_discovery.adjacency:
                        # Yup, link goes both ways
                        log.info('Found parallel adjacency');
                        self.adjacency[router1][router2] = ll.port1
                        self.adjacency[router2][router1] = ll.port2
                        # Fixed -- new link chosen to connect these
                        break
        else:
            # If we already consider these nodes connected, we can
            # ignore this link up.
            # Otherwise, we might be interested...
            if self.adjacency[router1][router2] is None:
                # These previously weren't connected.  If the link
                # exists in both directions, we consider them connected now.
                if flip(l) in core.openflow_discovery.adjacency:
                    # Link goes both ways -- connected!
                    self.adjacency[router1][router2] = l.port1
                    self.adjacency[router2][router1] = l.port2
                    log.info('Added adjacency: ' + str(router1) + '.'
                             + str(l.port1) + ' <-> ' + str(router2) + '.'
                             + str(l.port2))
                    if l.port1 in router1.connected_hosts:
                        log.debug('Deleted connected host (' + str(router1.connected_hosts[l.port1]) + ') on port ' + str(l.port1) + ' (host is actually a router)')
                        del router1.connected_hosts[l.port1]
                    if l.port2 in router2.connected_hosts:
                        log.debug('Deleted connected host (' + str(router2.connected_hosts[l.port2]) + ') on port ' + str(l.port2) + ' (host is actually a router)')
                        del router2.connected_hosts[l.port2]

    def _handle_ConnectionUp(self, event):
        router = self.routers.get(event.dpid)
        if router is None:
            # New router
            router = Router()
            router.dpid = event.dpid
            self.routers[event.dpid] = router
            log.info('Learned new router: ' + str(router))
            # sw.connect(event.connection)
        # else:
            # sw.connect(event.connection)
            
    def _handle_ConnectionDown (self, event):
        router = self.routers.get(event.dpid)
        if router is None:
            log.warn('Got ConnectionDown for unrecognized router')
        else:
            log.info('Router down: ' + str(self.routers[event.dpid]))
            del self.routers[event.dpid]
    
    def _handle_PacketIn(self, event):
        igmpPkt = event.parsed.find(pkt.igmp)
        if not igmpPkt is None:
            # IGMP packet - IPv4 Network
            # Make sure this router is known to the controller, drop the packet if now
            if not event.connection.dpid in self.routers:
                log.debug('Got IGMP packet from unrecognized router: ' + str(event.connection.dpid))
                # self.drop_packet(event)
                return
            
            # Determine the source IP of the packet
            receiving_router = self.routers[event.connection.dpid]
            log.info('Got IGMP packet at router: ' + str(receiving_router) + ' on port: ' + str(event.port))
            ipv4Pkt = event.parsed.find(pkt.ipv4)
            log.info(str(igmpPkt) + ' from Host: ' + str(ipv4Pkt.srcip))
            
            # Check to see if this IGMP message was received from a neighbouring router, and drop
            # it if so
            for neighbour in self.adjacency[receiving_router]:
                if self.adjacency[receiving_router][neighbour]:
                    log.info('IGMP packet received from neighbouring router, dropping packet.')
                    self.drop_packet(event)
                    return
            
            # The host must be directly connected to this router, learn its IP and port
            if not event.port in receiving_router.connected_hosts:
                receiving_router.connected_hosts[event.port] = ipv4Pkt.srcip
                log.info('Learned new host: ' + str(ipv4Pkt.srcip) + ' on port: ' + str(event.port))
        
        
def launch():
    core.registerNew(CastflowManager)


