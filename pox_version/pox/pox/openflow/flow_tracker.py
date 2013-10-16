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

AVERAGE_SMOOTHING_FACTOR = 0.6

class FlowTrackedSwitch(EventMixin):
    def __init__(self):
        self.connection = None
        self.is_connected = False
        self.dpid = None
        self._listeners = None
        self._connection_time = None
        self._last_query_response_time = None
        
        self.num_flows = 0
        # Maps are keyed by port number
        self.flow_total_byte_count = {}
        self.flow_interval_byte_count = {}
        self.flow_interval_bandwidth_Mbps = {}
        self.flow_average_bandwidth_Mbps = {}

    def __repr__(self):
        return dpid_to_str(self.dpid)

    def ignore_connection(self):
        if self.connection is not None:
            # log.debug('Disconnect %s' % (self.connection, ))
            self.connection.removeListeners(self._listeners)
            self.connection = None
            self.is_connected = False
            self._connection_time = None
            self._listeners = None

    def listen_on_connection(self, connection):
        if self.dpid is None:
            self.dpid = connection.dpid
        assert self.dpid == connection.dpid

        # log.debug('Connect %s' % (connection, ))
        self.connection = connection
        self.is_connected = True
        self._listeners = self.listenTo(connection)
        self._connection_time = time.time()
        self._last_query_response_time = self._connection_time

    def _handle_ConnectionDown(self, event):
        self.ignore_connection()
    
    def process_flow_stats(self, stats, reception_time):
        log.info(' ')
        log.info('Got FlowStatsReceived event from switch: ' + dpid_to_str(self.dpid) + ' at time: ' + str(reception_time))
        
        # Clear byte counts for this interval
        self.flow_interval_byte_count = {}
        self.num_flows = 0
        
        # Record the number of bytes transmitted through each port for this monitoring interval
        for flow_stat in stats:
            self.num_flows = self.num_flows + 1
            for action in flow_stat.actions:
                if isinstance(action, of.ofp_action_output):
                    if action.port in self.flow_interval_byte_count:
                        self.flow_interval_byte_count[action.port] = self.flow_interval_byte_count[action.port] + flow_stat.byte_count
                    else:
                        self.flow_interval_byte_count[action.port] = flow_stat.byte_count
                        
                    if action.port in self.flow_total_byte_count:
                        self.flow_total_byte_count[action.port] = self.flow_total_byte_count[action.port] + flow_stat.byte_count
                    else:
                        self.flow_total_byte_count[action.port] = flow_stat.byte_count
                    
                    # log.debug('Added ' + str(self.flow_interval_byte_count[action.port]) + ' byte flow on port ' + str(action.port))
        
        # Update bandwidth estimates
        for port_num in self.flow_interval_byte_count:
            # Update instant bandwidth
            self.flow_interval_bandwidth_Mbps[port_num] = ((self.flow_interval_byte_count[port_num] * 8.0) / 1048576.0) / (reception_time - self._last_query_response_time)
            # log.debug('Port: ' + str(port_num) + ' ' + str(self.flow_interval_bandwidth_Mbps[port_num]))
            # Update running average bandwidth
            if port_num in self.flow_average_bandwidth_Mbps:
                self.flow_average_bandwidth_Mbps[port_num] = (AVERAGE_SMOOTHING_FACTOR * self.flow_interval_bandwidth_Mbps[port_num]) + \
                    ((1 - AVERAGE_SMOOTHING_FACTOR) * self.flow_average_bandwidth_Mbps[port_num])
            else:
                self.flow_average_bandwidth_Mbps[port_num] = self.flow_interval_bandwidth_Mbps[port_num]
        
        # Update last response time
        self._last_query_response_time = reception_time
        
        # Print debug information
        for port_num in self.flow_interval_bandwidth_Mbps:
            if self.flow_interval_bandwidth_Mbps[port_num] > 0 or self.flow_average_bandwidth_Mbps[port_num] > 0:
                log.info('Port: ' + str(port_num) + ' Bandwidth Usage: ' + str(self.flow_interval_bandwidth_Mbps[port_num]) + \
                        ' Mbps (Average: ' + str(self.flow_average_bandwidth_Mbps[port_num]) + ' Mbps)')


class FlowTracker(EventMixin):
    _core_name = "openflow_flow_tracker"

    def __init__(self):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self)
            core.openflow_discovery.addListeners(self)
            self._module_init_time = time.time()
        
        self._got_first_connection = False  # Flag used to start the periodic query thread when the first ConnectionUp is received
        self._periodic_query_timer = None
        self.periodic_query_interval_seconds = 10
        
        self._module_init_time = 0
        
        # Map is keyed by dpid
        self.switches = {}
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'))
    
    def launch_stats_query(self):
        for switch_dpid in self.switches:
            if self.switches[switch_dpid].is_connected:
                self.switches[switch_dpid].connection.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))
                log.debug('Sent flow stats requests to switch: ' + dpid_to_str(switch_dpid))

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
        
        if not self._got_first_connection:
            self._got_first_connection = True
            self._periodic_query_timer = Timer(self.periodic_query_interval_seconds, self.launch_stats_query, recurring = True)
            
    def _handle_ConnectionDown (self, event):
        """Handler for ConnectionUp from the discovery module, which represents a switch leaving the network.
        """
        switch = self.switches.get(event.dpid)
        if switch is None:
            log.debug('Got ConnectionDown for unrecognized switch')
        else:
            log.debug('Lost connection with switch: ' + dpid_to_str(event.dpid))
            switch.ignore_connection()
    
    def _handle_FlowStatsReceived(self, event):
        if event.connection.dpid in self.switches:
            self.switches[event.connection.dpid].process_flow_stats(event.stats, time.time())
    
    
def launch():
    core.registerNew(FlowTracker)