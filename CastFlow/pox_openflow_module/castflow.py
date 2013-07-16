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

log = core.getLogger()


class Switch(EventMixin):

    def __init__(self):
        self.connection = None
        self.ports = None
        self.dpid = None
        self._listeners = None
        self._connected_at = None

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

        # Known switches:  [dpid] -> Switch
        self.switches = {}

        # Known links (contains Link objects defined in openflow.discovery
        # Not used in current code - replaced by adjacency map
        # self.links = []

        # Adjacency map:  [sw1][sw2] -> port from sw1 to sw2
        self.adjacency = defaultdict(lambda : defaultdict(lambda : \
                None))

        # ethaddr -> (switch, port)
        self.mac_map = {}

        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'
                             ))

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
        sw1 = self.switches[l.dpid1]
        sw2 = self.switches[l.dpid2]

        if event.removed:
            # This link no longer okay
            if sw2 in self.adjacency[sw1]:
                del self.adjacency[sw1][sw2]
            if sw1 in self.adjacency[sw2]:
                del self.adjacency[sw2][sw1]

            # But maybe there's another way to connect these...
            for ll in core.openflow_discovery.adjacency:
                if ll.dpid1 == l.dpid1 and ll.dpid2 == l.dpid2:
                    if flip(ll) in core.openflow_discovery.adjacency:
                        # Yup, link goes both ways
                        self.adjacency[sw1][sw2] = ll.port1
                        self.adjacency[sw2][sw1] = ll.port2
                        # Fixed -- new link chosen to connect these
                        break
        else:
            # If we already consider these nodes connected, we can
            # ignore this link up.
            # Otherwise, we might be interested...
            if self.adjacency[sw1][sw2] is None:
                # These previously weren't connected.  If the link
                # exists in both directions, we consider them connected now.
                if flip(l) in core.openflow_discovery.adjacency:
                    # Yup, link goes both ways -- connected!
                    self.adjacency[sw1][sw2] = l.port1
                    self.adjacency[sw2][sw1] = l.port2
                    log.info('Added adjacency: ' + str(sw1) + '.'
                             + str(l.port1) + ' <-> ' + str(sw2) + '.'
                             + str(l.port2))

        # If we have learned a MAC on this port which we now know to
        # be connected to a switch, unlearn it.
        bad_macs = set()
        for (mac, (sw, port)) in self.mac_map.iteritems():
            # print sw,sw1,port,l.port1
            if sw is sw1 and port == l.port1:
                if mac not in bad_macs:
                    log.debug('Unlearned %s', mac)
                    bad_macs.add(mac)
            if sw is sw2 and port == l.port2:
                if mac not in bad_macs:
                    log.debug('Unlearned %s', mac)
                    bad_macs.add(mac)
            for mac in bad_macs:
                del self.mac_map[mac]

    def _handle_ConnectionUp(self, event):
        sw = self.switches.get(event.dpid)
        if sw is None:
            # New switch
            sw = Switch()
            sw.dpid = event.dpid
            self.switches[event.dpid] = sw
            log.info('Learned new switch: ' + str(sw))
            # sw.connect(event.connection)
        # else:
            # sw.connect(event.connection)

def launch():
    core.registerNew(CastflowManager)


