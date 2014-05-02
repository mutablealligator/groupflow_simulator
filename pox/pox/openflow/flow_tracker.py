#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
A POX module which periodically queries the network to learn the following information:
- Bandwidth usage on all links in the network
- Number of flow table installations on all switches in the network

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
PERIODIC_QUERY_INTERVAL = 2 # Seconds

class FlowTrackedSwitch(EventMixin):
    def __init__(self, flow_tracker):
        self.flow_tracker = flow_tracker
        self.tracked_ports = [] # Only numbers in this list will have their utilization tracked
        self.connection = None
        self.is_connected = False
        self.dpid = None
        self.flow_removed_curr_interval = False
        self._listeners = None
        self._connection_time = None
        
        self._last_flow_stats_query_send_time = None 
        self._last_flow_stats_query_response_time = None
        self._last_flow_stats_query_network_time = None
        self._last_flow_stats_query_processing_time = None
        self._last_flow_stats_query_total_time = None
        
        self._last_port_stats_query_send_time = None
        
        self.num_flows = 0
        
        # Maps are keyed by port number
        # Flow maps record transmission statistics based on FlowStats queries
        self.flow_total_byte_count = {}
        self.flow_interval_byte_count = {}
        self.flow_interval_bandwidth_Mbps = {}
        self.flow_average_bandwidth_Mbps = {}
        self.flow_average_switch_load = 0
        
        # Port maps record reception statistics based on PortStats queries
        self.port_total_byte_count = {}
        self.port_interval_byte_count = {}
        self.port_interval_bandwidth_Mbps = {}
        self.port_average_bandwidth_Mbps = {}
        self.port_average_switch_load = 0
        
        self._periodic_query_timer = None

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
            if self._periodic_query_timer is not None:
                self._periodic_query_timer.cancel()
                self._periodic_query_timer = None

    def listen_on_connection(self, connection):
        if self.dpid is None:
            self.dpid = connection.dpid
        assert self.dpid == connection.dpid

        # log.debug('Connect %s' % (connection, ))
        self.connection = connection
        self.is_connected = True
        self._listeners = self.listenTo(connection)
        self._connection_time = time.time()
        self._last_flow_stats_query_response_time = self._connection_time
        self._last_port_stats_query_response_time = self._connection_time
        self._periodic_query_timer = Timer(self.flow_tracker.periodic_query_interval_seconds, self.launch_stats_query, recurring = True)

    def _handle_ConnectionDown(self, event):
        self.ignore_connection()
    
    def set_tracked_ports(self, tracked_ports):
        self.tracked_ports = tracked_ports
        log.debug('Switch ' + dpid_to_str(self.dpid) + ' set tracked ports: ' + str(tracked_ports))
        # Delete any stored state on ports which are no longer tracked
        keys_to_del = []
        for port_no in self.flow_interval_byte_count:
            if not port_no in self.tracked_ports:
                keys_to_del.append(port_no)
        for key in keys_to_del:
            del self.flow_total_byte_count[key]
            del self.flow_interval_byte_count[key]
            del self.flow_interval_bandwidth_Mbps[key]
            del self.flow_average_bandwidth_Mbps[key]
            
            del self.port_total_byte_count[key]
            del self.port_interval_byte_count[key]
            del self.port_interval_bandwidth_Mbps[key]
            del self.port_average_bandwidth_Mbps[key]
    
    def launch_stats_query(self):
        if self.is_connected:
            self.connection.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))
            self._last_flow_stats_query_send_time = time.time()
            self.connection.send(of.ofp_stats_request(body=of.ofp_port_stats_request()))
            self._last_port_stats_query_send_time = time.time()
            log.debug('Sent flow and port stats requests to switch: ' + dpid_to_str(self.dpid))
    
    def process_port_stats(self, stats, reception_time):
        log.info('== PortStatsReceived - Switch: ' + dpid_to_str(self.dpid) + ' - Time: ' + str(reception_time))
    
    def process_flow_stats(self, stats, reception_time):
        log.debug('== FlowStatsReceived - Switch: ' + dpid_to_str(self.dpid) + ' - Time: ' + str(reception_time))
        self._last_flow_stats_query_network_time = reception_time - self._last_flow_stats_query_send_time
        
        # Clear byte counts for this interval
        for port in self.flow_interval_byte_count:
            self.flow_interval_byte_count[port] = 0
        self.num_flows = 0
        
        curr_event_byte_count = {}
        
        # Check for new ports on the switch
        ports = self.connection.features.ports
        for port in ports:
            if port.port_no == of.OFPP_LOCAL or port.port_no == of.OFPP_CONTROLLER:
                continue
                
            if not port.port_no in self.tracked_ports:
                # log.debug('Switch ' + dpid_to_str(self.dpid) + ' detected untracked port: ' + str(port.port_no))
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
                    if action.port in self.tracked_ports:
                        if action.port in curr_event_byte_count:
                            curr_event_byte_count[action.port] = curr_event_byte_count[action.port] + flow_stat.byte_count
                        else:
                            curr_event_byte_count[action.port] = flow_stat.byte_count
                        
        # Determine the number of new bytes that appeared this interval, and set the flow removed flag to true if
        # any port count is lower than in the previous interval
        for port_num in curr_event_byte_count:
            if port_num in self.tracked_ports:
                if not port_num in self.flow_total_byte_count:
                    # Port has never appeared before
                    self.flow_total_byte_count[port_num] = curr_event_byte_count[port_num]
                    self.flow_interval_byte_count[port_num] = curr_event_byte_count[port_num]
                elif curr_event_byte_count[port_num] < self.flow_total_byte_count[port_num]:
                    # Byte count for this monitoring interval is less than previous interval, flow must have been removed
                    self.flow_total_byte_count[port_num] = curr_event_byte_count[port_num]
                    self.flow_interval_byte_count[port_num] = 0
                    self.flow_removed_curr_interval = True
                else:
                    self.flow_interval_byte_count[port_num] = curr_event_byte_count[port_num] - self.flow_total_byte_count[port_num]
                    self.flow_total_byte_count[port_num] = curr_event_byte_count[port_num]
        
        # Update bandwidth estimates if no flows were removed
        if not self.flow_removed_curr_interval:
            for port_num in self.flow_interval_byte_count:
                # Update instant bandwidth
                self.flow_interval_bandwidth_Mbps[port_num] = ((self.flow_interval_byte_count[port_num] * 8.0) / 1048576.0) / (reception_time - self._last_flow_stats_query_response_time)
                # Update running average bandwidth
                if port_num in self.flow_average_bandwidth_Mbps:
                    self.flow_average_bandwidth_Mbps[port_num] = min((self.flow_tracker.avg_smooth_factor * self.flow_interval_bandwidth_Mbps[port_num]) + \
                        ((1 - self.flow_tracker.avg_smooth_factor) * self.flow_average_bandwidth_Mbps[port_num]), self.flow_tracker.link_max_bw)
                else:
                    self.flow_average_bandwidth_Mbps[port_num] = min(self.flow_interval_bandwidth_Mbps[port_num], self.flow_tracker.link_max_bw)
        
        flow_average_switch_load = 0
        for port_num in self.flow_average_bandwidth_Mbps:
            flow_average_switch_load += self.flow_average_bandwidth_Mbps[port_num]
        self.flow_average_switch_load = flow_average_switch_load
        
        # Update last response time
        complete_processing_time = time.time()
        self._last_flow_stats_query_processing_time = complete_processing_time - reception_time
        self._last_flow_stats_query_total_time = complete_processing_time - self._last_flow_stats_query_send_time
        
        # Print log information to file
        if not self.flow_tracker._log_file is None:
            self.flow_tracker._log_file.write('FlowStats Switch:' + dpid_to_str(self.dpid) + ' NumFlows:' + str(self.num_flows) + ' IntervalLen:' + str(reception_time - self._last_flow_stats_query_response_time) + ' IntervalEndTime:' + str(reception_time) + ' ResponseTime:' + str(self._last_flow_stats_query_total_time) + ' NetworkTime:' + str(self._last_flow_stats_query_network_time) + ' ProcessingTime:' + str(self._last_flow_stats_query_processing_time) + ' AvgSwitchLoad:' + str(self.flow_average_switch_load) + '\n')

            for port_num in self.flow_interval_bandwidth_Mbps:
                self.flow_tracker._log_file.write('Port:' + str(port_num) + ' BytesThisInterval:' + str(self.flow_interval_byte_count[port_num])
                       + ' InstBandwidth:' + str(self.flow_interval_bandwidth_Mbps[port_num]) + ' AvgBandwidth:' + str(self.flow_average_bandwidth_Mbps[port_num])  + '\n')
                if(self.flow_average_bandwidth_Mbps[port_num] >= (self.flow_tracker.link_cong_threshold)):
                    log.warn('Congested link detected! Sw:' + dpid_to_str(self.dpid) + ' Port:' + str(port_num))
            
            #if self.flow_removed_curr_interval:
            #    log.warn('Flow removal detected!')
                
            self.flow_tracker._log_file.write('\n')
        
        self._last_flow_stats_query_response_time = reception_time
        self.flow_removed_curr_interval = False


class FlowTracker(EventMixin):
    _core_name = "openflow_flow_tracker"

    def __init__(self, query_interval, link_max_bw, link_cong_threshold, avg_smooth_factor, log_peak_usage):
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self, priority = 101)
            core.openflow_discovery.addListeners(self, priority = 101)
            core.openflow_igmp_manager.addListeners(self, priority = 101)
            self._module_init_time = time.time()
            self._log_file_name = datetime.datetime.now().strftime("flowtracker_%H-%M-%S_%B-%d_%Y.txt")
            log.info('Writing flow tracker info to file: ' + str(self._log_file_name))
            self._log_file = open(self._log_file_name, 'w') # TODO: Figure out how to properly close this on shutdown
        
        self._got_first_connection = False  # Flag used to start the periodic query thread when the first ConnectionUp is received
        self._peak_usage_output_timer = None
        
        self.periodic_query_interval_seconds = float(query_interval)
        self.link_max_bw = float(link_max_bw)
        self.link_cong_threshold = float(link_cong_threshold)
        self.avg_smooth_factor = float(avg_smooth_factor)
        self.log_peak_usage = float(log_peak_usage)
        
        log.info('Set QueryInterval:' + str(self.periodic_query_interval_seconds) + ' LinkMaxBw:' + str(self.link_max_bw) + 'Mbps LinkCongThreshold:' + str(self.link_cong_threshold) 
                + 'Mbps AvgSmoothFactor:' + str(self.avg_smooth_factor) + ' LogPeakUsage:' + str(self.log_peak_usage))
        
        self._module_init_time = 0
        self._log_file = None
        self._log_file_name = None
        
        # Map is keyed by dpid
        self.switches = {}
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_igmp_manager', 'openflow_discovery'))
    
    def termination_handler(self, signal, frame):
        if not self._log_file is None:
            self._log_file.close()
            self._log_file = None
            log.info('Termination signalled, closed log file: ' + str(self._log_file_name))
    
    def output_peak_usage(self):
        peak_usage = 0
        total_usage = 0
        num_links = 0
        for switch_dpid in self.switches:
            for port_no in self.switches[switch_dpid].flow_average_bandwidth_Mbps:
                if port_no == of.OFPP_LOCAL or port_no == of.OFPP_CONTROLLER:
                    continue
                total_usage += self.switches[switch_dpid].flow_average_bandwidth_Mbps[port_no]
                num_links += 1
                if self.switches[switch_dpid].flow_average_bandwidth_Mbps[port_no] > peak_usage:
                    peak_usage = self.switches[switch_dpid].flow_average_bandwidth_Mbps[port_no]

        log.info('Network peak link throughout (Mbps): ' + str(peak_usage))
        if num_links > 0:
            log.info('Network avg link throughout (Mbps): ' + str(total_usage / float(num_links)))

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
            if self.log_peak_usage:
                self._peak_usage_output_timer = Timer(self.periodic_query_interval_seconds / 1.5, self.output_peak_usage, recurring = True)
            
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
            
    def _handle_PortStatsReceived(self, event):
        if event.connection.dpid in self.switches:
            self.switches[event.connection.dpid].process_port_stats(event.stats, time.time())
    
    def _handle_MulticastTopoEvent(self, event):
        for switch1 in event.adjacency_map:
            if switch1 in self.switches:
                tracked_ports = []
                for switch2 in event.adjacency_map[switch1]:
                    if event.adjacency_map[switch1][switch2] is not None:
                        tracked_ports.append(event.adjacency_map[switch1][switch2])
                self.switches[switch1].set_tracked_ports(tracked_ports)
    
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