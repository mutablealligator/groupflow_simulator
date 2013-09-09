#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
A POX module implementation of the CastFlow clean slate multicast proposal, with modifications to support
management of group state using an IGMP manager module.

Implementation adapted from NOX-Classic CastFlow implementation provided by caioviel.

Depends on openflow.igmp_manager

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

class GroupFlowManager(EventMixin):
    def __init__(self):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self)
            core.openflow_igmp_manager.addListeners(self)

        self.topology_graph = []
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_igmp_manager'))
    
    def drop_packet(self, packet_in_event):
        """Drops the packet represented by the PacketInEvent without any flow table modification"""
        msg = of.ofp_packet_out()
        msg.data = packet_in_event.ofp
        msg.buffer_id = packet_in_event.ofp.buffer_id
        msg.in_port = packet_in_event.port
        msg.actions = []    # No actions = drop packet
        packet_in_event.connection.send(msg)

    def get_topo_debug_str(self):
        debug_str = '\n===== GroupFlow Learned Topology'
        for edge in self.topology_graph:
            debug_str += '\n(' + dpid_to_str(edge[0]) + ',' + dpid_to_str(edge[1]) + ')'
        return debug_str + '\n===== GroupFlow Learned Topology'
        
    def parse_topology_graph(self, adjacency_map):
        new_topo_graph = []
        for router1 in adjacency_map:
            for router2 in adjacency_map[router1]:
                new_topo_graph.append((router1, router2))
        self.topology_graph = new_topo_graph
    
    def _handle_PacketIn(self, event):
        """Handler for OpenFlow PacketIn events."""
        return
    
    def _handle_MulticastGroupEvent(self, event):
        log.info(event.debug_str())
    
    def _handle_MulticastTopoEvent(self, event):
        # log.info(event.debug_str())
        self.parse_topology_graph(event.adjacency_map)
        log.info(self.get_topo_debug_str())

def launch():
    core.registerNew(GroupFlowManager)