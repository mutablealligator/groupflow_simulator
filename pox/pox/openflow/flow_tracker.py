#!/usr/bin/python
# -*- coding: utf-8 -*-

#  Copyright 2014 Alexander Craig
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
A POX module which periodically queries the network to estimate link utilization.

Bandwidth usage is tracked on selected links in the network (using both FlowStats on the transmission side, and PortStats on the
receive side). Number of flow table installations is tracked on all switches in the network.

The following command line arguments are supported:

* query_interval: The length of time (in seconds) which should elapse between consecutive flow/port stats queries to switches. 
  Default: 2
* link_max_bw: The maximum bandwidth (in Mbps) of network links. Note that in the current implementation, all links must have
  uniform bandwidth.
  Default: 30
* link_cong_threshold: The utilization (in Mbps) above which a link should be considered as congested. Must be <= link_max_bw.
  Default: 28.5 
* avg_smooth_factor: The alpha value to use for the bandwidth estimation exponential average. The bandwidth utilization
  at each monitoring interval i is specified by BW_est_i = (alpha * BW_val_(i)) + ((1 - alpha) * BW_est_(i-1)). Higher alpha values
  therefore result in an average that more heavily weights recent samples.
  Default: 0.7  
* log_peak_usage: (True/False) If true, the peak and average utilization in the network are logged to debug.info at an interval of
  (query_interval / 1.5) seconds.
  Default: False.

Depends on openflow.discovery

Created on Oct 16, 2013

Author: Alexander Craig - alexcraig1@gmail.com
"""

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

PORT_STATS_GENERATE_LINK_EVENTS = False

# Note: These constants provide default values, which can be overridden by passing command
# line parameters when the module launches
OUTPUT_PEAK_USAGE = False
AVERAGE_SMOOTHING_FACTOR = 0.7
LINK_MAX_BANDWIDTH_MbPS = 30 # MegaBits per second
LINK_CONGESTION_THRESHOLD_MbPS = 0.95 * LINK_MAX_BANDWIDTH_MbPS
PERIODIC_QUERY_INTERVAL = 2 # Seconds

class LinkUtilizationEvent(Event):

    """Event which reports link status in the event of link congestion, or under under other conditions (TBD).
    
    This class contains the following public attributes:
    
    * router_dpid: The egress router of the link on which status is being reported
    * output_port: The output port of the link on which status is being reported
    * cong_threshold: The flow tracker's congestion threshold (in Mbps)
    * link_utilization: The utilization (in Mbps) of the link, as determined from PortStats on the ingress router.
      If port stats are not available on the ingress router, the utilization as determined from the
      egress router ports stats will be included instead.
    * stats_type: One of FLOW_STATS (0) or PORT_STATS (1). Records which type of stats reception triggered this event.
    * flow_map: A map of normalized bandwidth utilizations keyed by flow_cookie. This should be used to determine
      which flows should be replaced.
    ::
        
        flow_map[flow_cookie] = normalized bandwidth of flow with flow cookie flow_cookie
    """
    
    FLOW_STATS = 0
    PORT_STATS = 1
    
    def __init__ (self, router_dpid, output_port, cong_threshold, link_utilization, stats_type, flow_map):
        Event.__init__(self)
        self.router_dpid = router_dpid
        self.output_port = output_port
        self.cong_threshold = cong_threshold
        self.link_utilization = link_utilization
        self.stats_type = stats_type
        self.flow_map = flow_map


class FlowTrackedSwitch(EventMixin):
    """Class used to manage statistics querying and processing for a single OpenFlow switch.

    The FlowTracker module implements bandwidth tracking by managing a map of these objects.
    """

    def __init__(self, flow_tracker):
        """Initializes a new FlowTrackedSwitch"""
        self.flow_tracker = flow_tracker
        self.tracked_ports = [] # Only port numbers in this list will have their utilization tracked
        self.connection = None
        self.is_connected = False
        self.dpid = None
        self._listeners = None
        self._connection_time = None

        self._last_flow_stats_query_send_time = None
        self._last_flow_stats_query_response_time = None
        self._last_flow_stats_query_network_time = None
        self._last_flow_stats_query_processing_time = None
        self._last_flow_stats_query_total_time = None

        self._last_port_stats_query_send_time = None
        self._last_port_stats_query_response_time = None
        self._last_port_stats_query_network_time = None
        self._last_port_stats_query_processing_time = None
        self._last_port_stats_query_total_time = None

        self.num_flows = {} # Keyed by port number

        # Flow maps record transmission statistics based on FlowStats queries
        # Maps are keyed by port number
        # Each map contains a map of byte/bandwidth counts keyed by flow cookie (0 used for flows with no cookie)
        self.flow_total_byte_count = {}
        self.flow_interval_byte_count = {}
        self.flow_interval_bandwidth_Mbps = {}
        self.flow_average_bandwidth_Mbps = {}
        self.flow_total_average_bandwidth_Mbps = {} # This map stores the total estimated bandwidth on a per port basis
        self.flow_average_switch_load = 0

        # Port maps record reception statistics based on PortStats queries
        # Maps are keyed by port number
        # Byte/bandwidth counts are only recorded on a per port basis
        self.port_total_byte_count = {}
        self.port_interval_byte_count = {}
        self.port_interval_bandwidth_Mbps = {}
        self.port_average_bandwidth_Mbps = {}
        self.port_average_switch_load = 0

        self._periodic_query_timer = None

    def __repr__(self):
        """Returns string representation of the switch's data-plane identifier."""
        return dpid_to_str(self.dpid)

    def ignore_connection(self):
        """Disconnects listener methods on statistics queries, and stops the periodic query timer."""
        if self.connection is not None:
            log.debug('Disconnect %s' % (self.connection, ))
            self.connection.removeListeners(self._listeners)
            self.connection = None
            self.is_connected = False
            self._connection_time = None
            self._listeners = None
            self._last_port_stats_query_response_time = None
            self._last_flow_stats_query_response_time = None

            if self._periodic_query_timer is not None:
                self._periodic_query_timer.cancel()
                self._periodic_query_timer = None

    def listen_on_connection(self, connection):
        """Configures listener methods to handle query responses, and starts the periodic query timer.
        
        * connection: Connection object for this switch (usually obtained from a ConnectionUp event)
        """
        if self.dpid is None:
            self.dpid = connection.dpid
        assert self.dpid == connection.dpid

        log.debug('Connect %s' % (connection, ))
        self.connection = connection
        self.is_connected = True
        self._listeners = self.listenTo(connection)
        self._connection_time = time.time()
        self._last_flow_stats_query_response_time = None
        self._last_port_stats_query_response_time = None
        self._periodic_query_timer = Timer(self.flow_tracker.periodic_query_interval_seconds, self.launch_stats_query,
            recurring=True)

    def _handle_ConnectionDown(self, event):
        """Handler called when a ConnectionDown event is generated by the POX core."""
        self.ignore_connection()

    def set_tracked_ports(self, tracked_ports):
        """Sets the port numbers on which bandwidth utilization should be tracked for this switch.
        
        * tracked_ports: List of integer port numbers on which utilization should be tracked for this switch
        """
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
        """Sends an OpenFlow FlowStatsRequest and PortStatsRequest to the switch associated with this object."""
        if self.is_connected:
            self.connection.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))
            self._last_flow_stats_query_send_time = time.time()
            self.connection.send(of.ofp_stats_request(body=of.ofp_port_stats_request()))
            self._last_port_stats_query_send_time = time.time()
            log.debug('Sent flow and port stats requests to switch: ' + dpid_to_str(self.dpid))

    def process_port_stats(self, stats, reception_time):
        """Processes a PortStats response to a PortStatsRequest.

        Port stats are processed to determine bandwidth utilization from the receiving side of a link. This method was
        chosen to overcome limitations in Mininet's link emulation technique, which causes FlowStats to overestimate
        the utilization of a link when a link becomes congested. PortStats should always give an accurate count of the
        bytes received on a particular port even in congestion conditions, but the utilization cannot be determined on
        a per-flow basis using PortStats messages.

        An exponential moving average is used to smooth bandwidth estimates, where the alpha of the exponential average
        is set by flow_tracker.avg_smooth_factor.
        """
        if not self.is_connected:
            return

        log.debug('== PortStatsReceived - Switch: ' + dpid_to_str(self.dpid) + ' - Time: ' + str(reception_time))
        
        self._last_port_stats_query_network_time = reception_time - self._last_port_stats_query_send_time

        # Clear byte counts for this interval
        for port in self.port_interval_byte_count:
            self.port_interval_byte_count[port] = 0
        curr_event_byte_count = {}

        # Check for new ports on the switch
        ports = self.connection.features.ports
        invalid_stat_ports = []     # Ports added to this list will not have their bandwidth averages updated for this interval
        for port in ports:
            if port.port_no == of.OFPP_LOCAL or port.port_no == of.OFPP_CONTROLLER:
                continue

            if not port.port_no in self.tracked_ports:
                continue

            if not port.port_no in self.port_total_byte_count:
                invalid_stat_ports.append(
                    port.port_no) # Port bandwidth statistics are not updated on the first interval the port appears
                self.port_total_byte_count[port.port_no] = 0
                self.port_interval_byte_count[port.port_no] = 0
                self.port_interval_bandwidth_Mbps[port.port_no] = 0
                self.port_average_bandwidth_Mbps[port.port_no] = 0

        # Record the number of bytes transmitted through each port for this monitoring interval
        for port_stat in stats:
            if port_stat.port_no in self.tracked_ports:
                if port_stat.port_no in curr_event_byte_count:
                    curr_event_byte_count[port_stat.port_no] = curr_event_byte_count[
                                                               port_stat.port_no] + port_stat.rx_bytes
                else:
                    curr_event_byte_count[port_stat.port_no] = port_stat.rx_bytes

        # Determine the number of new bytes that appeared this interval, and set the flow removed flag to true if
        # any port count is lower than in the previous interval
        for port_num in curr_event_byte_count:
            if port_num in self.tracked_ports:
                if not port_num in self.port_total_byte_count:
                    # Port has never appeared before
                    self.port_total_byte_count[port_num] = curr_event_byte_count[port_num]
                    self.port_interval_byte_count[port_num] = curr_event_byte_count[port_num]
                elif curr_event_byte_count[port_num] < self.port_total_byte_count[port_num]:
                    # Byte count for this monitoring interval is less than previous interval, flow must have been removed
                    self.port_total_byte_count[port_num] = curr_event_byte_count[port_num]
                    self.port_interval_byte_count[port_num] = 0
                    invalid_stat_ports.append(port_num)
                else:
                    self.port_interval_byte_count[port_num] = (curr_event_byte_count[port_num]
                            - self.port_total_byte_count[port_num])
                    self.port_total_byte_count[port_num] = curr_event_byte_count[port_num]

        # Skip further processing if this was the first measurement interval, or if the measurement interval had an unreasonable duration
        if self._last_port_stats_query_response_time is None:
            self._last_port_stats_query_response_time = reception_time
            return
        interval_len = reception_time - self._last_port_stats_query_response_time
        if (interval_len < (0.5 * self.flow_tracker.periodic_query_interval_seconds)
                or interval_len > (2 * self.flow_tracker.periodic_query_interval_seconds)):
            self._last_port_stats_query_response_time = reception_time
            return

        # Update bandwidth estimates for valid ports
        for port_num in self.port_interval_byte_count:
            if port_num in invalid_stat_ports:
                continue

            # Update instant bandwidth - Note that this is capped at 5% above the link's maximum supported bandwidth
            self.port_interval_bandwidth_Mbps[port_num] = min(((self.port_interval_byte_count[
                                                            port_num] * 8.0) / 1048576.0) / (interval_len),
                                                            self.flow_tracker.link_max_bw * 1.05)
            # Update running average bandwidth
            if port_num in self.port_average_bandwidth_Mbps:
                self.port_average_bandwidth_Mbps[port_num] = (self.flow_tracker.avg_smooth_factor *
                                                              self.port_interval_bandwidth_Mbps[port_num]) +\
                                                             ((1 - self.flow_tracker.avg_smooth_factor) *
                                                              self.port_average_bandwidth_Mbps[port_num])
            else:
                self.port_average_bandwidth_Mbps[port_num] = self.port_interval_bandwidth_Mbps[port_num]

        port_average_switch_load = 0
        for port_num in self.port_average_bandwidth_Mbps:
            port_average_switch_load += self.port_average_bandwidth_Mbps[port_num]
        self.port_average_switch_load = port_average_switch_load

        # Update last response time
        complete_processing_time = time.time()
        self._last_port_stats_query_processing_time = complete_processing_time - reception_time
        self._last_port_stats_query_total_time = complete_processing_time - self._last_port_stats_query_send_time

        # Print log information to file
        if not self.flow_tracker._log_file is None:
            # Note: NumFlows is only included here so that the PortStats logs will exactly match the format of FlowStats
            # (makes for easier log processing)
            self.flow_tracker._log_file.write('PortStats Switch:' + dpid_to_str(self.dpid) + ' NumFlows:' + str(
                sum(self.num_flows.values())) + ' IntervalLen:' + str(
                interval_len) + ' IntervalEndTime:' + str(reception_time) 
                + ' ResponseTime:' + str(self._last_port_stats_query_total_time) + ' NetworkTime:' + str(
                self._last_port_stats_query_network_time) + ' ProcessingTime:' + str(self._last_port_stats_query_processing_time) 
                + ' AvgSwitchLoad:' + str(self.port_average_switch_load) + '\n')

            for port_num in self.port_interval_bandwidth_Mbps:
                self.flow_tracker._log_file.write(
                    'PSPort:' + str(port_num) + ' BytesThisInterval:' + str(self.port_interval_byte_count[port_num])
                    + ' InstBandwidth:' + str(self.port_interval_bandwidth_Mbps[port_num]) + ' AvgBandwidth:' + str(
                        self.port_average_bandwidth_Mbps[port_num]) + '\n')
                
                if PORT_STATS_GENERATE_LINK_EVENTS:
                    if(self.port_average_bandwidth_Mbps[port_num] >= (self.flow_tracker.link_cong_threshold)):
                        # Generate an event if the link is congested
                        # First, get the switch on the other side of this link
                        send_switch_dpid = None
                        send_port = None
                        for link in core.openflow_discovery.adjacency:
                            if link.dpid1 == self.dpid and link.port1 == port_num:
                                send_switch_dpid = link.dpid2
                                send_port = link.port2
                                break
                                
                        if send_switch_dpid is None or send_port is None:
                            continue
                            
                        log.debug('PortStats: Congested link detected! SendSw: ' + dpid_to_str(send_switch_dpid) + ' Port: ' + str(send_port))
                        event = LinkUtilizationEvent(send_switch_dpid, send_port, self.flow_tracker.link_cong_threshold,
                                self.port_average_bandwidth_Mbps[port_num], LinkUtilizationEvent.PORT_STATS,
                                self.flow_tracker.switches[send_switch_dpid].flow_average_bandwidth_Mbps[port_num])
                        self.flow_tracker.raiseEvent(event)

            self.flow_tracker._log_file.write('\n')

        self._last_port_stats_query_response_time = reception_time


    def process_flow_stats(self, stats, reception_time):
        """Processes a FlowStats response to a FlowStatsRequest.

        Flow stats are processed to determine bandwidth utilization from the transmission side of a link. This method
        can produce inaccurate measurements when using Mininet link emulation. In particular, the statistics returned
        by emulated switches will not properly detect dropped packets, and the byte counts returned in FlowStats
        responses will overestimate the actual bytes forwarded when the link becomes congested. FlowStats are recorded
        on a per-flow basis, allowing the module the percentage of link utilization contributed by each flow in the
        network. Flows are differentiated by their controller assigned flow cookie. Flows with no cookie default to a
        cookie value of 0, and the FlowTracker module will consider all flows without a cookie as a single flow with
        cookie value 0.

        An exponential moving average is used to smooth bandwidth estimates, where the alpha of the exponential average
        is set by flow_tracker.avg_smooth_factor.
        """
        if not self.is_connected:
            return

        log.debug('== FlowStatsReceived - Switch: ' + dpid_to_str(self.dpid) + ' - Time: ' + str(reception_time))
        self._last_flow_stats_query_network_time = reception_time - self._last_flow_stats_query_send_time

        # Clear byte counts for this interval
        for port in self.flow_interval_byte_count:
            self.flow_interval_byte_count[port] = {}
            
        num_flows = {}
        for port_num in self.tracked_ports:
            num_flows[port_num] = 0

        curr_event_byte_count = {}

        # Check for new ports on the switch
        ports = self.connection.features.ports
        for port in ports:
            if port.port_no == of.OFPP_LOCAL or port.port_no == of.OFPP_CONTROLLER:
                continue

            if not port.port_no in self.tracked_ports:
                continue

            if not port.port_no in self.flow_total_byte_count:
                self.flow_total_byte_count[port.port_no] = {}
                self.flow_interval_byte_count[port.port_no] = {}
                self.flow_interval_bandwidth_Mbps[port.port_no] = {}
                self.flow_average_bandwidth_Mbps[port.port_no] = {}

        # Record the number of bytes transmitted through each port for this monitoring interval
        for flow_stat in stats:
            for action in flow_stat.actions:
                if isinstance(action, of.ofp_action_output):
                    if action.port in self.tracked_ports:
                        if flow_stat.cookie != 0:
                            num_flows[action.port] += 1
                        
                        # log.info('Got flow on tracked port with cookie: ' + str(flow_stat.cookie))
                        if action.port in curr_event_byte_count:
                            if flow_stat.cookie in curr_event_byte_count[action.port]:
                                curr_event_byte_count[action.port][flow_stat.cookie] = \
                                        curr_event_byte_count[action.port][flow_stat.cookie] + flow_stat.byte_count
                            else:
                                curr_event_byte_count[action.port][flow_stat.cookie] = flow_stat.byte_count
                        else:
                            curr_event_byte_count[action.port] = {}
                            curr_event_byte_count[action.port][flow_stat.cookie] = flow_stat.byte_count

        # Determine the number of new bytes that appeared this interval
        negative_byte_count = False
        for port_num in curr_event_byte_count:
            if port_num in self.tracked_ports:
                if not port_num in self.flow_total_byte_count:
                    # Port has never appeared before
                    self.flow_total_byte_count[port_num] = {}
                    self.flow_interval_byte_count[port_num] = {}
                    for flow_cookie in curr_event_byte_count[port_num]:
                        self.flow_total_byte_count[port_num][flow_cookie] = curr_event_byte_count[port_num][flow_cookie]
                        self.flow_interval_byte_count[port_num][flow_cookie] = curr_event_byte_count[port_num][flow_cookie]
                else:
                    for flow_cookie in curr_event_byte_count[port_num]:
                        if flow_cookie not in self.flow_total_byte_count[port_num]:
                            # Flow has not appeared before
                            self.flow_total_byte_count[port_num][flow_cookie] = curr_event_byte_count[port_num][
                                                                                flow_cookie]
                            self.flow_interval_byte_count[port_num][flow_cookie] = curr_event_byte_count[port_num][
                                                                                   flow_cookie]
                        else:
                            if curr_event_byte_count[port_num][flow_cookie] < self.flow_total_byte_count[port_num][flow_cookie]:
                                # TODO: Find a better way to handle the case where a flow reports less bytes forwarded than the previous interval
                                log.info('Switch: ' + dpid_to_str(self.dpid) + ' Port: ' + str(port_num) + ' FlowCookie: ' + str(flow_cookie) + '\n\tReported negative byte count: '
                                        + str(curr_event_byte_count[port_num][flow_cookie] - self.flow_total_byte_count[port_num][flow_cookie]))
                                self.flow_total_byte_count[port_num][flow_cookie] = curr_event_byte_count[port_num][flow_cookie]
                                self.flow_interval_byte_count[port_num][flow_cookie] = 0
                                negative_byte_count = True
                            else:
                                self.flow_interval_byte_count[port_num][flow_cookie] = (curr_event_byte_count[port_num][
                                                                                       flow_cookie] -
                                                                                       self.flow_total_byte_count[port_num][
                                                                                       flow_cookie])
                                self.flow_total_byte_count[port_num][flow_cookie] = curr_event_byte_count[port_num][
                                                                                    flow_cookie]

        # Remove counters for flows that were removed in this interval
        flows_to_remove = []
        for port_num in self.flow_total_byte_count:
            for flow_cookie in self.flow_total_byte_count[port_num]:
                if port_num not in curr_event_byte_count or flow_cookie not in curr_event_byte_count[port_num]:
                    flows_to_remove.append((port_num, flow_cookie))
        for removal in flows_to_remove:
            log.debug('Removing bandwidth counters for port: ' + str(removal[0]) + ' flow cookie: ' + str(removal[1]))
            if removal[1] in self.flow_interval_byte_count[removal[0]]:
                del self.flow_interval_byte_count[removal[0]][removal[1]]
            if removal[1] in self.flow_total_byte_count[removal[0]]:
                del self.flow_total_byte_count[removal[0]][removal[1]]
            if removal[1] in self.flow_interval_bandwidth_Mbps[removal[0]]:
                del self.flow_interval_bandwidth_Mbps[removal[0]][removal[1]]
            if removal[1] in self.flow_average_bandwidth_Mbps[removal[0]]:
                del self.flow_average_bandwidth_Mbps[removal[0]][removal[1]]

        # Skip further processing if this was the first measurement interval, or if the measurement interval had an unreasonable duration
        if negative_byte_count or self._last_flow_stats_query_response_time is None:
            self._last_flow_stats_query_response_time = reception_time
            return
        interval_len = reception_time - self._last_flow_stats_query_response_time
        if interval_len < (0.5 * self.flow_tracker.periodic_query_interval_seconds) or interval_len > (
        2 * self.flow_tracker.periodic_query_interval_seconds):
            self._last_flow_stats_query_response_time = reception_time
            return

        # Update bandwidth estimates
        self.flow_total_average_bandwidth_Mbps = {}
        for port_num in self.flow_interval_byte_count:
            if port_num not in self.flow_interval_bandwidth_Mbps:
                self.flow_interval_bandwidth_Mbps[port_num] = {}
                self.flow_average_bandwidth_Mbps[port_num] = {}

            for flow_cookie in self.flow_interval_byte_count[port_num]:
                if flow_cookie not in self.flow_interval_bandwidth_Mbps[port_num]:
                    self.flow_interval_bandwidth_Mbps[port_num][flow_cookie] = 0
                    self.flow_average_bandwidth_Mbps[port_num][flow_cookie] = 0

                # Update instant bandwidth - Note that this is capped at 5% above the link's maximum supported bandwidth
                self.flow_interval_bandwidth_Mbps[port_num][flow_cookie] = \
                        min(((self.flow_interval_byte_count[port_num][flow_cookie] * 8.0) / 1048576.0) / (interval_len),
                        self.flow_tracker.link_max_bw * 1.05)
                        
                # Update running average bandwidth
                self.flow_average_bandwidth_Mbps[port_num][flow_cookie] = \
                    (self.flow_tracker.avg_smooth_factor * self.flow_interval_bandwidth_Mbps[port_num][flow_cookie]) + \
                    ((1 - self.flow_tracker.avg_smooth_factor) * self.flow_average_bandwidth_Mbps[port_num][flow_cookie])
                    
                
                if(self.flow_average_bandwidth_Mbps[port_num][flow_cookie] < 0):
                    log.warn('FlowStats reported negative bandwidth (' + str(self.flow_average_bandwidth_Mbps[port_num][flow_cookie]) + ' Mbps) '
                            + 'on \n\tSwitch: ' + dpid_to_str(self.dpid) + ' Port: ' + str(port_num) + ' Flow Cookie: ' + str(flow_cookie)
                            + '\n\tInterval Len: ' + str(interval_len))
            
            self.flow_total_average_bandwidth_Mbps[port_num] = sum(self.flow_average_bandwidth_Mbps[port_num].itervalues())

        flow_average_switch_load = 0
        for port_num in self.flow_average_bandwidth_Mbps:
            for flow_cookie in self.flow_average_bandwidth_Mbps[port_num]:
                flow_average_switch_load += self.flow_average_bandwidth_Mbps[port_num][flow_cookie]
        self.flow_average_switch_load = flow_average_switch_load

        # Update last response time
        complete_processing_time = time.time()
        self._last_flow_stats_query_processing_time = complete_processing_time - reception_time
        self._last_flow_stats_query_total_time = complete_processing_time - self._last_flow_stats_query_send_time

        # Print log information to file
        self.num_flows = num_flows
        if not self.flow_tracker._log_file is None:
            self.flow_tracker._log_file.write('FlowStats Switch:' + dpid_to_str(self.dpid) + ' NumFlows:' + str(
                sum(self.num_flows.values())) + ' IntervalLen:' + str(interval_len) + ' IntervalEndTime:' + str(
                reception_time) + ' ResponseTime:' + str(
                self._last_flow_stats_query_total_time) + ' NetworkTime:' + str(
                self._last_flow_stats_query_network_time) + ' ProcessingTime:' + str(
                self._last_flow_stats_query_processing_time) + ' AvgSwitchLoad:' + str(
                self.flow_average_switch_load) + '\n')

            for port_num in self.flow_interval_bandwidth_Mbps:
                for flow_cookie in self.flow_interval_bandwidth_Mbps[port_num]:
                    self.flow_tracker._log_file.write(
                        'FSPort:' + str(port_num) + ' FlowCookie: ' + str(flow_cookie) + ' BytesThisInterval:' + str(
                            self.flow_interval_byte_count[port_num][flow_cookie])
                        + ' InstBandwidth:' + str(
                            self.flow_interval_bandwidth_Mbps[port_num][flow_cookie]) + ' AvgBandwidth:' + str(
                            self.flow_average_bandwidth_Mbps[port_num][flow_cookie]) + '\n')
                
                link_util_Mbps = self.flow_tracker.get_link_utilization_mbps(self.dpid, port_num)
                # Generate an event if the link is congested
                if link_util_Mbps >= self.flow_tracker.link_cong_threshold:
                    event = LinkUtilizationEvent(self.dpid, port_num, self.flow_tracker.link_cong_threshold,
                            link_util_Mbps, LinkUtilizationEvent.FLOW_STATS, self.flow_average_bandwidth_Mbps[port_num])
                    self.flow_tracker.raiseEvent(event)
                
                # Log to console if a link is fully utilized
                if link_util_Mbps >= self.flow_tracker.link_max_bw:
                    # Get the DPID of the switch on the other side of the link
                    receive_switch_dpid = None
                    for link in core.openflow_discovery.adjacency:
                        if link.dpid1 == self.dpid and link.port1 == port_num:
                            receive_switch_dpid = link.dpid2
                            break
                    
                    # Calculate the minimum node degree of the two switches
                    min_node_degree = min(len(self.tracked_ports), len(self.flow_tracker.switches[receive_switch_dpid].tracked_ports))
                    
                    log.warn('FlowStats: Fully utilized link detected! SendSw:' + dpid_to_str(self.dpid) + ' Port:' + str(port_num) 
                            + ' MinNodeDegree:' + str(min_node_degree) + ' UtilMbps:' + str(link_util_Mbps))

            self.flow_tracker._log_file.write('\n')

        self._last_flow_stats_query_response_time = reception_time


class FlowTracker(EventMixin):

    """Module which implements bandwidth utilization tracking by managing a map of FlowTrackedSwitches."""
    
    _core_name = "openflow_flow_tracker"
    
    _eventMixin_events = set([
        LinkUtilizationEvent
    ])

    def __init__(self, query_interval, link_max_bw, link_cong_threshold, avg_smooth_factor, log_peak_usage):
        """Initializes the FlowTracker module, and configures all required listeners once dependencies have loaded."""
        # Listen to dependencies
        def startup():
            core.openflow.addListeners(self, priority=101)
            core.openflow_discovery.addListeners(self, priority=101)
            core.openflow_igmp_manager.addListeners(self, priority=101)
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

        log.info('Set QueryInterval:' + str(self.periodic_query_interval_seconds) + ' LinkMaxBw:' + str(
            self.link_max_bw) + 'Mbps LinkCongThreshold:' + str(self.link_cong_threshold)
                 + 'Mbps AvgSmoothFactor:' + str(self.avg_smooth_factor) + ' LogPeakUsage:' + str(self.log_peak_usage))

        self._module_init_time = 0
        self._log_file = None
        self._log_file_name = None

        # Map is keyed by dpid
        self.switches = {}

        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_igmp_manager', 'openflow_discovery'))

    def termination_handler(self, signal, frame):
        """Method to cleanly terminate the module (i.e. close the log file) when a SIGINT signal is received.

        This function is typically called by the BenchmarkTerminator module.
        """
        if not self._log_file is None:
            # Write out the final topology of the network
            self._log_file.flush()
            self._log_file.write('Final Network Topology:\n')
            for link in core.openflow_discovery.adjacency:
                if link.dpid1 in self.switches and link.port1 in self.switches[link.dpid1].tracked_ports:
                    self._log_file.write(str(link.dpid1) + ' P:' + str(link.port1) + ' -> ' + str(link.dpid2) 
                            + ' P:' + str(link.port2) + ' U:' + str(self.get_link_utilization_normalized(link.dpid1, link.port1))
                            + ' NF:' + str(self.switches[link.dpid1].num_flows[link.port1]) + '\n')
            self._log_file.close()
            self._log_file = None
            log.info('Termination signalled, closed log file: ' + str(self._log_file_name))

    def output_peak_usage(self):
        """Outputs the current peak utilization and average link utilization to log.info"""
        peak_usage = 0
        total_usage = 0
        num_links = 0
        for switch_dpid in self.switches:
            if not self.switches[switch_dpid].is_connected:
                continue

            for port_no in self.switches[switch_dpid].tracked_ports:
                if port_no == of.OFPP_LOCAL or port_no == of.OFPP_CONTROLLER:
                    continue
                util = self.get_link_utilization_mbps(switch_dpid, port_no)
                total_usage += util
                num_links += 1
                if util > peak_usage:
                    peak_usage = util
        
        cur_time = time.time()
        log.info('Network peak link throughput (Mbps): ' + str(peak_usage) + ' Time:' + str(cur_time))
        if num_links > 0:
            log.info('Network avg link throughput (Mbps): ' + str(total_usage / float(num_links)) + ' Time:' + str(cur_time))

    def _handle_ConnectionUp(self, event):
        """Handler for ConnectionUp from the discovery module, which represents a new switch joining the network."""
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
                self._peak_usage_output_timer = Timer(self.periodic_query_interval_seconds / 1.5, self.output_peak_usage
                    , recurring=True)

    def _handle_ConnectionDown(self, event):
        """Handler for ConnectionDown from the discovery module, which represents a switch leaving the network."""
        switch = self.switches.get(event.dpid)
        if switch is None:
            log.debug('Got ConnectionDown for unrecognized switch')
        else:
            log.debug('Lost connection with switch: ' + dpid_to_str(event.dpid))
            switch.ignore_connection()

    def _handle_FlowStatsReceived(self, event):
        """Forwards the flow statistics contained in the FlowStats event to the appropriate FlowTrackedSwitch."""
        if event.connection.dpid in self.switches:
            self.switches[event.connection.dpid].process_flow_stats(event.stats, time.time())

    def _handle_PortStatsReceived(self, event):
        """Forwards the port statistics contained in the FlowStats event to the appropriate FlowTrackedSwitch."""
        if event.connection.dpid in self.switches:
            self.switches[event.connection.dpid].process_port_stats(event.stats, time.time())

    def _handle_MulticastTopoEvent(self, event):
        """Processes a topology event generated by the IGMPManager module, and enables utilization tracking on all inter-switch links."""
        for switch1 in event.adjacency_map:
            if switch1 in self.switches:
                tracked_ports = []
                for switch2 in event.adjacency_map[switch1]:
                    if event.adjacency_map[switch1][switch2] is not None:
                        tracked_ports.append(event.adjacency_map[switch1][switch2])
                self.switches[switch1].set_tracked_ports(tracked_ports)

    def get_link_utilization_mbps(self, switch_dpid, output_port):
        """Returns the current estimated utilization (in Mbps) on the specified switch and output port.

        If a utilization is available based on port stats from the receive side of the specified link, this value will
        be returned (as port stats are more reliable in Mininet than flow stats). If port stats are not available (which
        would occur when the opposite side of the link is not being tracked) then a utilization estimate derived from
        flow stats will be returned.
        """
        # First, get the switch on the other side of this link
        receive_switch_dpid = None
        receive_port = None
        for link in core.openflow_discovery.adjacency:
            if link.dpid1 == switch_dpid and link.port1 == output_port:
                receive_switch_dpid = link.dpid2
                receive_port = link.port2
                break

        if receive_switch_dpid is None:
            # Reception statistics unavailable, use the transmission statistics if available
            log.warn("PortStats unavailable for Switch: " + dpid_to_str(switch_dpid) + ' Port: ' + str(output_port))
            if switch_dpid in self.switches:
                if output_port in self.switches[switch_dpid].flow_total_average_bandwidth_Mbps:
                    return self.switches[switch_dpid].flow_total_average_bandwidth_Mbps[output_port]
            return 0    # TODO: May want to throw exception here

        if receive_switch_dpid in self.switches:
            if receive_port in self.switches[receive_switch_dpid].port_average_bandwidth_Mbps:
                return self.switches[receive_switch_dpid].port_average_bandwidth_Mbps[receive_port]

        return 0    # TODO: May want to throw exception here

    def get_link_utilization_normalized(self, switch_dpid, output_port):
        """Returns the current estimated utilization (as a normalized value between 0 and 1) on a particular link.
        
        * switch_dpid: The dataplane identifier of the switch on the transmitting side of the link
        * output_port: The output port on switch with dpid switch_dpid corresponding to the link
        
        Note: Current implementation assumes all links have equal maximum bandwidth which is defined by self.link_max_bw
        """
        return self.get_link_utilization_mbps(switch_dpid, output_port) / self.link_max_bw

    def get_flow_utilization_normalized(self, switch_dpid, output_port, flow_cookie):
        """Returns the percentage of link utilization on a particular link contributed by a particular flow (as a normalized value between 0 and 1).
        
        * switch_dpid: The dataplane identifier of the switch on the transmitting side of the link
        * output_port: The output port on switch with dpid switch_dpid corresponding to the link
        * flow_cookie: The flow cookie assigned to the flow of interest
        """
        flow_bw_usage = 0
        if switch_dpid in self.switches:
            if output_port in self.switches[switch_dpid].flow_average_bandwidth_Mbps:
                if flow_cookie in self.switches[switch_dpid].flow_average_bandwidth_Mbps[output_port]:
                    flow_bw_usage = self.switches[switch_dpid].flow_average_bandwidth_Mbps[output_port][flow_cookie]
        
        total_link_bw_usage = 0
        if switch_dpid in self.switches:
            if output_port in self.switches[switch_dpid].flow_total_average_bandwidth_Mbps:
                total_link_bw_usage = self.switches[switch_dpid].flow_total_average_bandwidth_Mbps[output_port]
        
        if flow_bw_usage == 0 or total_link_bw_usage == 0:
            return 0
        else:
            return flow_bw_usage / total_link_bw_usage
    
    def get_max_flow_utilization(self, flow_cookie):
        """Returns the maximum estimated utilization (in Mbps) for the specified flow cookie across all tracked links in the network.
        
        * flow_cookie: The flow cookie assigned to the flow of interest
        """
        max_util_mbps = 0
        for switch_dpid in self.switches:
            for output_port in self.switches[switch_dpid].flow_average_bandwidth_Mbps:
                if flow_cookie in self.switches[switch_dpid].flow_average_bandwidth_Mbps[output_port]:
                    flow_util_mbps = self.switches[switch_dpid].flow_average_bandwidth_Mbps[output_port][flow_cookie]
                    if flow_util_mbps > max_util_mbps:
                        max_util_mbps = flow_util_mbps
        
        return max_util_mbps
    
    def get_num_tracked_links(self):
        """Returns the total number of links tracked by this module."""
        tracked_links = 0
        for switch_dpid in self.switches:
            tracked_links += len(self.switches[switch_dpid].tracked_ports)
        return tracked_links
        

def launch(query_interval=PERIODIC_QUERY_INTERVAL, link_max_bw=LINK_MAX_BANDWIDTH_MbPS,
           link_cong_threshold=LINK_CONGESTION_THRESHOLD_MbPS, avg_smooth_factor=AVERAGE_SMOOTHING_FACTOR,
           log_peak_usage=False):
    # Method called by the POX core when launching the module
    flow_tracker = FlowTracker(float(query_interval), float(link_max_bw), float(link_cong_threshold),
        float(avg_smooth_factor), bool(log_peak_usage))
    core.register('openflow_flow_tracker', flow_tracker)