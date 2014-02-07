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
import datetime

log = core.getLogger()

# Note: These constants provide default values, which can be overridden by passing command
# line parameters when the module launches
OUTPUT_PEAK_USAGE = False
AVERAGE_SMOOTHING_FACTOR = 0.7
LINK_MAX_BANDWIDTH_MbPS = 30 # MegaBits per second
LINK_CONGESTION_THRESHOLD_MbPS = 0.95 * LINK_MAX_BANDWIDTH_MbPS
PERIODIC_QUERY_INTERVAL = 4 # Seconds

class FlowTrackedSwitch(EventMixin):
    def __init__(self, flow_tracker):
        self.flow_tracker = flow_tracker
        self.connection = None
        self.is_connected = False
        self.dpid = None
        self.flow_removed_curr_interval = False
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
        log.debug('== FlowStatsReceived - Switch: ' + dpid_to_str(self.dpid) + ' - Time: ' + str(reception_time))
        
        # Clear byte counts for this interval
        for port in self.flow_interval_byte_count:
            self.flow_interval_byte_count[port] = 0
        self.num_flows = 0
        
        curr_event_byte_count = {}
        
        # Check for new ports on the switch
        ports = self.connection.features.ports
        for port in ports:
            if port.port_no == of.OFPP_LOCAL:
                continue
                
            if not port.port_no in self.flow_total_byte_count:
                self.flow_total_byte_count[port.port_no] = 0
                self.flow_interval_byte_count[port.port_no] = 0
                self.flow_interval_bandwidth_Mbps[port.port_no] = 0
                self.flow_average_bandwidth_Mbps[port.port_no] = 0
        
        # Record the number of bytes transmitted through each port for this monitoring interval
        for flow_stat in stats:
            self.num_flows = self.num_flows + 1
            for action in flow_stat.actions:
                if isinstance(action, of.ofp_action_output):
                    if action.port in curr_event_byte_count:
                        curr_event_byte_count[action.port] = curr_event_byte_count[action.port] + flow_stat.byte_count
                    else:
                        curr_event_byte_count[action.port] = flow_stat.byte_count
                        
        # Determine the number of new bytes that appeared this interval, and set the flow removed flag to true if
        # any port count is lower than in the previous interval
        for port_num in curr_event_byte_count:
            if not port_num in self.flow_total_byte_count:
                self.flow_total_byte_count[port_num] = curr_event_byte_count[port_num]
                self.flow_interval_byte_count[port_num] = curr_event_byte_count[port_num]
                continue
                
            if curr_event_byte_count[port_num] < self.flow_total_byte_count[port_num]:
                self.flow_total_byte_count[port_num] = curr_event_byte_count[port_num]
                self.flow_interval_byte_count[port_num] = 0
                self.flow_removed_curr_interval = True
                continue
            
            self.flow_interval_byte_count[port_num] = curr_event_byte_count[port_num] - self.flow_total_byte_count[port_num]
            self.flow_total_byte_count[port_num] = curr_event_byte_count[port_num]
        
        # Update bandwidth estimates if no flows were removed
        if not self.flow_removed_curr_interval:
            for port_num in self.flow_interval_byte_count:
                # Update instant bandwidth
                self.flow_interval_bandwidth_Mbps[port_num] = ((self.flow_interval_byte_count[port_num] * 8.0) / 1048576.0) / (reception_time - self._last_query_response_time)
                # log.debug('Port: ' + str(port_num) + ' ' + str(self.flow_interval_bandwidth_Mbps[port_num]))
                # Update running average bandwidth
                if port_num in self.flow_average_bandwidth_Mbps:
                    self.flow_average_bandwidth_Mbps[port_num] = min((self.flow_tracker.avg_smooth_factor * self.flow_interval_bandwidth_Mbps[port_num]) + \
                        ((1 - self.flow_tracker.avg_smooth_factor) * self.flow_average_bandwidth_Mbps[port_num]), self.flow_tracker.link_max_bw)
                else:
                    self.flow_average_bandwidth_Mbps[port_num] = min(self.flow_interval_bandwidth_Mbps[port_num], self.flow_tracker.link_max_bw)
        
        # Update last response time
        self._last_query_response_time = reception_time
        
        # Print debug information
        # log.info('Num Flows: ' + str(self.num_flows))
        # if(self.flow_removed_curr_interval):
        #     log.info('Removed flows detected')
        # for port_num in self.flow_interval_bandwidth_Mbps:
        #    if self.flow_interval_bandwidth_Mbps[port_num] > 0 or self.flow_average_bandwidth_Mbps[port_num] > 0:
        #        log.info('Port ' + str(port_num) + ' - ' + str(self.flow_average_bandwidth_Mbps[port_num]) + ' Mbps - Bytes This Interval: ' + str(self.flow_interval_byte_count[port_num]))
        
        # Print log information to file
        if not self.flow_tracker._log_file is None:
            self.flow_tracker._log_file.write('FlowStats Switch:' + dpid_to_str(self.dpid) + ' NumFlows:' + str(self.num_flows) + ' IntervalLen:' + str(reception_time - self._last_query_response_time) + ' IntervalEndTime:' + str(reception_time) + '\n')
            #for port_num in curr_event_byte_count:
            #    self.flow_tracker._log_file.write('Port:' + str(port_num) + ' BytesThisEvent: ' + str(curr_event_byte_count[port_num]) + '\n')
            #    log.info('Switch:' + dpid_to_str(self.dpid) + 'Port:' + str(port_num) + ' BytesThisEvent: ' + str(curr_event_byte_count[port_num]))
            
            for port_num in self.flow_interval_bandwidth_Mbps:
                self.flow_tracker._log_file.write('Port:' + str(port_num) + ' BytesThisInterval:' + str(self.flow_interval_byte_count[port_num])
                       + ' InstBandwidth:' + str(self.flow_interval_bandwidth_Mbps[port_num]) + ' AvgBandwidth:' + str(self.flow_average_bandwidth_Mbps[port_num])  + '\n')
                #log.warn('Port:' + str(port_num) + ' BytesThisInterval:' + str(self.flow_interval_byte_count[port_num])
                #       + ' InstBandwidth:' + str(self.flow_interval_bandwidth_Mbps[port_num]) + ' AvgBandwidth:' + str(self.flow_average_bandwidth_Mbps[port_num])  + '\n')
                if(self.flow_average_bandwidth_Mbps[port_num] >= (self.flow_tracker.link_cong_threshold)):
                    log.warn('Congested link detected! Sw:' + dpid_to_str(self.dpid) + ' Port:' + str(port_num))
                    
            self.flow_tracker._log_file.write('\n')
        
        self.flow_removed_curr_interval = False


class FlowTracker(EventMixin):
    _core_name = "openflow_flow_tracker"

    def __init__(self, query_interval, link_max_bw, link_cong_threshold, avg_smooth_factor, log_peak_usage):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self)
            core.openflow_discovery.addListeners(self)
            self._module_init_time = time.time()
            self._log_file_name = datetime.datetime.now().strftime("flowtracker_%H-%M-%S_%B-%d_%Y.txt")
            log.info('Writing flow tracker info to file: ' + str(self._log_file_name))
            self._log_file = open(self._log_file_name, 'w') # TODO: Figure out how to properly close this on shutdown
        
        self._got_first_connection = False  # Flag used to start the periodic query thread when the first ConnectionUp is received
        self._periodic_query_timer = None
        self._peak_usage_output_timer = None
        
        self.periodic_query_interval_seconds = query_interval
        self.link_max_bw = link_max_bw
        self.link_cong_threshold = link_cong_threshold
        self.avg_smooth_factor = avg_smooth_factor
        self.log_peak_usage = log_peak_usage
        
        log.info('Set QueryInterval:' + str(self.periodic_query_interval_seconds) + ' LinkMaxBw:' + str(self.link_max_bw) + 'Mbps LinkCongThreshold:' + str(self.link_cong_threshold) 
                + 'Mbps AvgSmoothFactor:' + str(self.avg_smooth_factor) + ' LogPeakUsage:' + str(self.log_peak_usage))
        
        self._module_init_time = 0
        self._log_file = None
        self._log_file_name = None
        
        # Map is keyed by dpid
        self.switches = {}
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_discovery'))
    
    def termination_handler(self, signal, frame):
        if not self._log_file is None:
            self._log_file.close()
            self._log_file = None
            log.info('Termination signalled, closed log file: ' + str(self._log_file_name))
    
    def launch_stats_query(self):
        for switch_dpid in self.switches:
            if self.switches[switch_dpid].is_connected:
                self.switches[switch_dpid].connection.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))
                log.debug('Sent flow stats requests to switch: ' + dpid_to_str(switch_dpid))
    
    def output_peak_usage(self):
        peak_usage = 0
        for switch_dpid in self.switches:
            for port_no in self.switches[switch_dpid].flow_average_bandwidth_Mbps:
                if self.switches[switch_dpid].flow_average_bandwidth_Mbps[port_no] > peak_usage:
                    peak_usage = self.switches[switch_dpid].flow_average_bandwidth_Mbps[port_no]
        log.info('Network peak link throughout (MBps): ' + str(peak_usage))

    def _handle_ConnectionUp(self, event):
        """Handler for ConnectionUp from the discovery module, which represent new switches joining the network.
        """
        if not event.dpid in self.switches:
            # New switch
            switch = FlowTrackedSwitch(self)
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
            if self.log_peak_usage:
                self._peak_usage_output_timer = Timer(self.periodic_query_interval_seconds / 3, self.output_peak_usage, recurring = True)
            
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
            
    def get_link_utilization_mbps(self, switch_dpid, output_port):
        if switch_dpid in self.switches:
            if output_port in self.switches[switch_dpid].flow_average_bandwidth_Mbps:
                return self.switches[switch_dpid].flow_average_bandwidth_Mbps[output_port]
            else:
                return 0    # TODO: May want to throw exception here
        else:
            return 0    # TODO: May want to throw exception here
    
    def get_link_utilization_normalized(self, switch_dpid, output_port):
        ''' Note: Current implementation assumes all links have equal maximum bandwidth
            which is defined by self.link_max_bw'''
        return self.get_link_utilization_mbps(switch_dpid, output_port) / self.link_max_bw
    
def launch(query_interval = PERIODIC_QUERY_INTERVAL, link_max_bw = LINK_MAX_BANDWIDTH_MbPS, link_cong_threshold = LINK_CONGESTION_THRESHOLD_MbPS, avg_smooth_factor = AVERAGE_SMOOTHING_FACTOR, log_peak_usage = False):
    flow_tracker = FlowTracker(float(query_interval), float(link_max_bw), float(link_cong_threshold), float(avg_smooth_factor), bool(log_peak_usage))
    core.register('openflow_flow_tracker', flow_tracker)