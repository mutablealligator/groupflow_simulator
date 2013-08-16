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
from pox.lib.addresses import IPAddr, EthAddr

log = core.getLogger()


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

        # Known routers:  [dpid] -> Router
        self.routers = {}

        # Known links (contains Link objects defined in openflow.discovery
        # Not used in current code - replaced by adjacency map
        # self.links = []

        # Adjacency map:  [router1][router2] -> port from router1 to router2
        self.adjacency = defaultdict(lambda : defaultdict(lambda : \
                None))

        # ethaddr -> (router, port)
        self.mac_map = {}

        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'))

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
                    # Yup, link goes both ways -- connected!
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

            # This was replaced with the connected_hosts dictionaries
            
            # If we have learned a MAC on this port which we now know to
            # be connected to a router, unlearn it.
            # bad_macs = set()
            # for (mac, (router, port)) in self.mac_map.iteritems():
            #    # print sw,router1,port,l.port1
            #    if router is router1 and port == l.port1:
            #        if mac not in bad_macs:
            #            log.debug('Unlearned %s', mac)
            #            bad_macs.add(mac)
            #    if router is router2 and port == l.port2:
            #        if mac not in bad_macs:
            #            log.debug('Unlearned %s', mac)
            #            bad_macs.add(mac)
            #    for mac in bad_macs:
            #        del self.mac_map[mac]

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
            if not event.connection.dpid in self.routers:
                log.debug('Got IGMP packet from unrecognized router: ' + str(event.connection.dpid))
                return
            receiving_router = self.routers[event.connection.dpid]
            for neighbour in self.adjacency[receiving_router]:
                if self.adjacency[receiving_router][neighbour]:
                    log.debug('IGMP packet received from neighbouring router.')
                    return
            log.info('Got IGMP packet at router: ' + str(receiving_router) + ' on port: ' + str(event.port))
            ipv4Pkt = event.parsed.find(pkt.ipv4)
            log.info(str(igmpPkt) + ' from Host: ' + str(ipv4Pkt.srcip))
            if not event.port in receiving_router.connected_hosts:
                receiving_router.connected_hosts[event.port] = ipv4Pkt.srcip
                log.info('Learned new host: ' + str(ipv4Pkt.srcip) + ' on port: ' + str(event.port))
        
        
def launch():
    core.registerNew(CastflowManager)


