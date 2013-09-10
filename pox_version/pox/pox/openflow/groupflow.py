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
from sets import Set
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

class MulticastPath:
    def __init__(self, src_ip, src_router_dpid, ingress_port, dst_mcast_address, groupflow_manager):
        self.src_ip = src_ip
        self.ingress_port = ingress_port
        self.src_router_dpid = src_router_dpid
        self.dst_mcast_address = dst_mcast_address
        self.mst = None
        self.last_installed_mst = None
        self.last_installed_receivers = None
        
        self.rules_installed = False
        self.latest_rules_installed = False
        
        self.weighted_topo_graph = []
        self.node_list = []
        self.receivers = []                 # Tuples of (router_dpid, port)
        self.groupflow_manager = groupflow_manager
    
    # Implementation taken directly from the Castflow implementation
    def calc_mst(self):
        self.calc_link_weights()
        nodes = self.node_list
        edges = self.weighted_topo_graph
        
        conn = defaultdict( list )
        for n1,n2,c in edges:
            conn[ n1 ].append( (c, n1, n2) )
            conn[ n2 ].append( (c, n2, n1) )
        mst = []
        used = set([self.src_router_dpid])
        usable_edges = conn[self.src_router_dpid][:]
        heapify( usable_edges )

        while usable_edges:

            cost, n1, n2 = heappop( usable_edges )
            if n2 not in used:
                used.add( n2 )
                mst.append( ( n1, n2) )
                for e in conn[ n2 ]:
                    if e[ 2 ] not in used:
                        heappush( usable_edges, e )
                        
        self.mst = mst
        self.latest_rules_installed = False
        
        log.debug('Calculated MST for source at router_dpid: ' + dpid_to_str(self.src_router_dpid))
        for edge in self.mst:
            log.debug(dpid_to_str(edge[0]) + ' -> ' + dpid_to_str(edge[1]))
    
    def update_node_weight(self, node, curr_topo_graph, weighted_topo_graph, weight, prev_node = None):
        for edge in curr_topo_graph:
            if edge[0] == node:
                if edge[1] == prev_node:
                    continue;
                    
                updated_existing = False
                found_lower_weight = False
                for weighted_edge in weighted_topo_graph:
                    if weighted_edge[0] == node and weighted_edge[2] <= weight:
                        found_lower_weight = True
                        break
                    elif weighted_edge[0] == node and weighted_edge[2] > weight:
                        weighted_edge[2] = weight
                        updated_existing = True
                if not updated_existing:
                    weighted_topo_graph.append([edge[0], edge[1], weight])
                
                if not found_lower_weight:
                    weighted_topo_graph = self.update_node_weight(edge[1], curr_topo_graph, weighted_topo_graph, weight + 1, edge[0])
        
        return weighted_topo_graph
    
    # Implementation taken directly from the Castflow implementation
    def calc_link_weights(self):
        curr_topo_graph = self.groupflow_manager.topology_graph
        self.node_list = list(self.groupflow_manager.node_set)
        self.weighted_topo_graph = self.update_node_weight(self.src_router_dpid, curr_topo_graph, \
                self.weighted_topo_graph, 0)
        
        #log.debug('Calculated link weights for source at router_dpid: ' + dpid_to_str(self.src_router_dpid))
        #for edge in self.weighted_topo_graph:
        #    log.debug(dpid_to_str(edge[0]) + ' -> ' + dpid_to_str(edge[1]) + ' W: ' + str(edge[2]))
    
    def install_openflow_rules(self):
        self.calc_mst()
        
        if self.latest_rules_installed:
            return
            
        if self.rules_installed:
            self.remove_openflow_rules()
        
        reception_state = self.groupflow_manager.get_reception_state(self.dst_mcast_address, self.src_ip)
        outgoing_rules = defaultdict(lambda : None)
        
        for edge in self.mst:
            if edge[0] in outgoing_rules:
                # Add the output action to an existing rule if it has already been generated
                output_port = self.groupflow_manager.adjacency[edge[0]][edge[1]]
                outgoing_rules[edge[0]].actions.append(of.ofp_action_output(port = output_port))
                log.info('ER: Configured router ' + dpid_to_str(edge[0]) + ' to forward group ' + \
                    str(self.dst_mcast_address) + ' to next router ' + \
                    dpid_to_str(edge[1]) + ' over port: ' + str(output_port))
            else:
                # Otherwise, generate a new flow mod
                msg = of.ofp_flow_mod()
                msg.match.dl_type = 0x800   # IPV4
                msg.match.nw_dst = self.dst_mcast_address
                msg.match.nw_src = self.src_ip
                msg.match.in_port = None
                output_port = self.groupflow_manager.adjacency[edge[0]][edge[1]]
                msg.actions.append(of.ofp_action_output(port = output_port))
                outgoing_rules[edge[0]] = msg
                log.info('NR: Configured router ' + dpid_to_str(edge[0]) + ' to forward group ' + \
                    str(self.dst_mcast_address) + ' to next router ' + \
                    dpid_to_str(edge[1]) + ' over port: ' + str(output_port))
        
        for receiver in reception_state:
            if receiver[0] in outgoing_rules:
                # Add the output action to an existing rule if it has already been generated
                output_port = receiver[1]
                outgoing_rules[receiver[0]].actions.append(of.ofp_action_output(port = output_port))
                log.info('ER: Configured router ' + dpid_to_str(receiver[0]) + ' to forward group ' + \
                        str(self.dst_mcast_address) + ' to network over port: ' + str(output_port))
            else:
                # Otherwise, generate a new flow mod
                msg = of.ofp_flow_mod()
                msg.match.dl_type = 0x800   # IPV4
                msg.match.nw_dst = self.dst_mcast_address
                msg.match.nw_src = self.src_ip
                msg.match.in_port = None
                
                # TODO - Figure this... port matching is just ignored for now
                #if(edge[0] == self.src_router_dpid):
                #    msg.match.in_port = self.ingress_port
                #else:
                #    # TODO - Determine the ingress port for this edge
                
                output_port = receiver[1]
                msg.actions.append(of.ofp_action_output(port = output_port))
                outgoing_rules[receiver[0]] = msg
                log.info('NR: Configured router ' + dpid_to_str(receiver[0]) + ' to forward group ' + \
                        str(self.dst_mcast_address) + ' to network over port: ' + str(output_port))
        
        # Setup empty rules for any router not involved in this path
        for router_dpid in self.node_list:
            if not router_dpid in outgoing_rules:
                msg = of.ofp_flow_mod()
                msg.match.dl_type = 0x800   # IPV4
                msg.match.nw_dst = self.dst_mcast_address
                msg.match.nw_src = self.src_ip
                msg.match.in_port = None
                outgoing_rules[router_dpid] = msg
                log.info('NR: Configured router ' + dpid_to_str(router_dpid) + ' to ignore traffic for group ' + str(self.dst_mcast_address))
        
        for router_dpid in outgoing_rules:
            core.openflow.getConnection(router_dpid).send(outgoing_rules[router_dpid])
        
        self.last_installed_mst = self.mst
        self.last_installed_receivers = reception_state
        self.rules_installed = True
        self.latest_rules_installed = True
            
    def remove_openflow_rules(self):
        if not self.rules_installed:
            return
        
        log.info('Removing rules on all routers for Group: ' + str(self.dst_mcast_address) + ' Source: ' + str(self.src_ip))
        for router_dpid in self.node_list:
            msg = of.ofp_flow_mod()
            msg.match.dl_type = 0x800   # IPV4
            msg.match.nw_dst = self.dst_mcast_address
            msg.match.nw_src = self.src_ip
            msg.match.in_port = None
            msg.command = of.OFPFC_DELETE
            core.openflow.getConnection(router_dpid).send(msg)
        
        self.rules_installed = False
        self.latest_rules_installed = False
        
    def calculate_mst(self):
        return
    


class GroupFlowManager(EventMixin):
    _core_name = "openflow_groupflow"
    
    def __init__(self):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self)
            core.openflow_igmp_manager.addListeners(self)

        self.adjacency = defaultdict(lambda : defaultdict(lambda : None))
        self.topology_graph = []
        self.node_set = Set()
        # self.multicast_paths[mcast_group][src_ip]
        self.multicast_paths = defaultdict(lambda : defaultdict(lambda : None))
        
        # Desired reception state as delivered by the IGMP manager, keyed by the dpid of the router for which
        # the reception state applies
        self.desired_reception_state = defaultdict(lambda : None)
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_igmp_manager'))
    
    def get_reception_state(self, mcast_group, src_ip):
        log.debug('Calculating reception state for mcast group: ' + str(mcast_group) + ' Source: ' + str(src_ip))
        reception_state = []
        for router_dpid in self.desired_reception_state:
            log.debug('Considering router: ' + dpid_to_str(router_dpid))
            if mcast_group in self.desired_reception_state[router_dpid]:
                for port in self.desired_reception_state[router_dpid][mcast_group]:
                    if len(self.desired_reception_state[router_dpid][mcast_group][port]) == 0:
                        reception_state.append((router_dpid, port))
                        log.debug('Reception from all sources desired on port: ' + str(port))
                    elif src_ip in self.desired_reception_state[router_dpid][mcast_group][port]:
                        reception_state.append((router_dpid, port))
                        log.debug('Reception from specific source desired on port: ' + str(port))
        else:
            return reception_state

    
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
        new_node_list = []
        for router1 in adjacency_map:
            for router2 in adjacency_map[router1]:
                new_topo_graph.append((router1, router2))
                if not router2 in new_node_list:
                    new_node_list.append(router2)
            if not router1 in new_node_list:
                new_node_list.append(router1)
        self.topology_graph = new_topo_graph
        self.node_set = Set(new_node_list)
    
    def _handle_PacketIn(self, event):
        router_dpid = event.connection.dpid
        if not router_dpid in self.node_set:
            # log.debug('Got packet from unrecognized router.')
            return  # Ignore packets from unrecognized routers
            
        igmp_pkt = event.parsed.find(pkt.igmp)
        if not igmp_pkt is None:
            return # IGMP packets should be ignored by this module
            
        ipv4_pkt = event.parsed.find(pkt.ipv4)
        if not ipv4_pkt is None:
            # ==== IPv4 Packet ====
            # Check the destination address to see if this is a multicast packet
            if ipv4_pkt.dstip.inNetwork('224.0.0.0/4'):
                log.info('Got multicast packet from new source. Router: ' + dpid_to_str(event.dpid) + ' Port: ' + str(event.port))
                log.info('Reception state for this group:')
                group_reception = self.get_reception_state(ipv4_pkt.dstip, ipv4_pkt.srcip)
                for receiver in group_reception:
                    log.info('Multicast Receiver: ' + dpid_to_str(receiver[0]) + ':' + str(receiver[1]))

                pathSetup = MulticastPath(ipv4_pkt.srcip, router_dpid, event.port, ipv4_pkt.dstip, self)
                pathSetup.install_openflow_rules()
                self.multicast_paths[ipv4_pkt.dstip][ipv4_pkt.srcip] = pathSetup
    
    def _handle_MulticastGroupEvent(self, event):
        # log.info(event.debug_str())
        self.desired_reception_state[event.router_dpid] = event.desired_reception
        log.info('Set new reception state for router: ' + dpid_to_str(event.router_dpid))
        
        # TODO: Rebuild multicast trees for relevant multicast groups
        log.info('Recalculating paths due to new receiver')
        for multicast_addr in self.desired_reception_state[event.router_dpid]:
            if multicast_addr in self.multicast_paths:
                log.info('Recalculating paths for group ' + str(multicast_addr))
                for source in self.multicast_paths[multicast_addr]:
                    log.info('Recalculating paths for group ' + str(multicast_addr) + ' Source: ' + str(source))
                    self.multicast_paths[multicast_addr][source].install_openflow_rules()
            else:
                log.info('No existing sources for group ' + str(multicast_addr)) 
    
    def _handle_MulticastTopoEvent(self, event):
        # log.info(event.debug_str())
        self.adjacency = event.adjacency_map
        self.parse_topology_graph(event.adjacency_map)
        # log.info(self.get_topo_debug_str())
        # TODO: Rebuild multicast trees for all multicast groups.

def launch():
    core.registerNew(GroupFlowManager)