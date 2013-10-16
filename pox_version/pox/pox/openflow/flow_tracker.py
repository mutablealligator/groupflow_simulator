#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
A POX module which periodically queries the network to learn the following information:
- Bandwidth usage on all links in the network
- Number of flow table installations on all routers in the network
- Queue state of all links in the network

Depends on openflow.discovery

Created on Oct 16, 2013
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
from pox.lib.packet.igmpv3 import *   # Required for various IGMP variable constants
from pox.lib.packet.ethernet import *
import pox.openflow.libopenflow_01 as of
from pox.lib.addresses import IPAddr, EthAddr
from pox.lib.recoco import Timer
import time

log = core.getLogger()

class FlowTrackedSwitch(EventMixin):
    def __init__(self):
        self.connection = None

        self.dpid = None
        self._listeners = None
        self._connection_time = None

    def __repr__(self):
        return dpid_to_str(self.dpid)

    def ignore_connection(self):
        if self.connection is not None:
            # log.debug('Disconnect %s' % (self.connection, ))
            self.connection.removeListeners(self._listeners)
            self.connection = None
            self._connection_time = None
            self._listeners = None

    def listen_on_connection(self, connection):
        if self.dpid is None:
            self.dpid = connection.dpid
        assert self.dpid == connection.dpid

        # log.debug('Connect %s' % (connection, ))
        self.connection = connection
        self._listeners = self.listenTo(connection)
        self._connection_time = time.time()

    def _handle_ConnectionDown(self, event):
        self.disconnect()


class FlowTracker(EventMixin):
    _core_name = "openflow_flow_tracker"

    def __init__(self):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self)
            core.openflow_discovery.addListeners(self)
        
        # Map is keyed by dpid
        self.switches = {}
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'))

    def _handle_ConnectionUp(self, event):
        """Handler for ConnectionUp from the discovery module, which represent new switches joining the network.
        """
        if not event.dpid in self.switches:
            # New switch
            switch = FlowTrackedSwitch()
            switch.dpid = event.dpid
            self.switches[event.dpid] = switch
            log.debug('Learned new switch: ' + dpid_to_str(switch.dpid))
            switch.listen_on_connection(event.connection)
        else:
            log.debug('Restablished connection with switch: ' + dpid_to_str(event.dpid))
            self.switches[event.dpid].listen_on_connection(event.connection)
            
    def _handle_ConnectionDown (self, event):
        """Handler for ConnectionUp from the discovery module, which represents a switch leaving the network.
        """
        switch = self.switches.get(event.dpid)
        if switch is None:
            log.debug('Got ConnectionDown for unrecognized switch')
        else:
            log.debug('Lost connection with switch: ' + dpid_to_str(event.dpid))
            switch.ignore_connection()
    
    
def launch():
    core.registerNew(FlowTracker)