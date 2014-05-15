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
This module allows event traces to be produced by the IGMP Manager and Groupflow modules, with the goal of
tracing various event processing times for benchmarking and evaluation purposes.

This module does not support any command line arguments.

Created on Oct 28th, 2013

Author: Alexander Craig - alexcraig1@gmail.com
"""

import time
import datetime
from pox.core import core
from pox.lib.revent import *
from pox.lib.util import dpid_to_str
import pox.lib.packet as pkt

USE_TIME_TIME = 0   # Benchmark using time.time()
USE_TIME_CLOCK = 1  # Benchmark using time.clock()

# Set this constant to one of the above options
# time.clock() is the default if no valid option is set here
TIMING_MODE = USE_TIME_TIME

log = core.getLogger()

class TraceEvent(EventMixin):
    
    """Superclass for trace events. Stores only the event ID and time of the event's creation.
    
    Subclassed by IGMPTraceEvent and GroupFlowTraceEvent.
    """

    def __init__(self, event_id=0):
        """Initializes a new event with the specified integer event_id."""
        self.init_time = self.get_curr_time()
        self.event_id = event_id

    def get_curr_time(self):
        """Returns the current time, using the method selected by the TIMING_MODE constant.

        This method is used to record time values in all subclasses of this class. Valid values
        for TIMING_MODE are USE_TIME_TIME (which uses time.time() to get the current time) and
        USE_TIME_CLOCK (which uses time.clock() to get the current time)
        """
        global TIMING_MODE
        if TIMING_MODE == USE_TIME_TIME:
            return time.time()
        if TIMING_MODE == USE_TIME_CLOCK:
            return time.clock()

        # time.clock() is the default behaviour if no valid option is specified
        return time.clock()


class IGMPTraceEvent(TraceEvent):

    """Trace event which records processing times associated with a single IGMP packet.

    The following data is recorded:
    
    * router_dpid: Data plane identifier of the router which received the packet
    * igmp_msg_type: The type of IGMP message processed (see constants defined in pox.lib.packet.igmpv3)
    * igmp_group_records: A list of tuples specifying the group records contained in the IGMP packet.
      Tuples are of the form (group_record_type, multicast_address).
      Valid group records types are defined in pox.lib.packet.igmpv3.
    * num_igmp_group_records: Number of group records contained in the IGMP packet
    * igmp_processing_start_time: Time at which the packet was identified as an IGMP packet
    * igmp_prcessing_end_time: Time at which all IGMP processing associated with the IGMP packet was completed
    """

    def __init__(self, event_id, router_dpid):
        """Initializes a new IGMPTraceEvent with the specified event_id and router_dpid"""
        TraceEvent.__init__(self, event_id)
        self.router_dpid = router_dpid
        self.igmp_msg_type = None
        self.igmp_group_records = []
        self.num_igmp_group_records = 0
        self.igmp_processing_start_time = 0
        self.igmp_proecssing_end_time = 0
        self._processing_complete = False

    def set_igmp_start_time(self, igmp_packet_in_event):
        """Processes a PacketIn event containing an IGMP packet, and sets associated data fields in the trace event.

        Fields which are populated by this method:
        * igmp_msg_type, igmp_group_records, num_igmp_group_records, igmp_processing_start_time
        
        For best benchmarking accuracy, this method should be called as soon as the PacketIn is determined to contain an IGMP packet
        """
        igmp_pkt = igmp_packet_in_event.parsed.find(pkt.igmpv3)
        self.igmp_msg_type = igmp_pkt.msg_type
        for igmp_group_record in igmp_pkt.group_records:
            self.igmp_group_records.append((igmp_group_record.record_type, igmp_group_record.multicast_address))
        self.num_igmp_group_records = len(igmp_pkt.group_records)
        self.igmp_processing_start_time = self.get_curr_time()

    def set_igmp_end_time(self):
        """Records the current time as the time at which IGMP processing associated with this event was completed."""
        self.igmp_processing_end_time = self.get_curr_time()
        self._processing_complete = True

    def get_igmp_processing_time(self):
        """Returns the length of time (in seconds) associated with IGMP processing for this event.

        Returns None if no end time has been set for the event.
        """
        if not self._processing_complete:
            return None

        return self.igmp_processing_end_time - self.igmp_processing_start_time

    def get_log_str(self):
        """Returns a plain-text representation of the event that will be used when the event is serialized to a log file.

        Note that the current implementation does not serialize complete IGMP group records.
        """
        return_string = str(self.event_id) + ' - ' + '{:10.8f}'.format(self.init_time) + '\n'
        return_string += 'Router: ' + dpid_to_str(self.router_dpid) + ' IGMP Msg type: ' + str(
            self.igmp_msg_type) + ' Num Records: ' + str(self.num_igmp_group_records) + '\n'
        if self._processing_complete:
            return_string += 'IGMP processing time: ' + '{:10.8f}'.format(
                self.get_igmp_processing_time() * 1000) + ' ms\n'
        return return_string


class GroupFlowTraceEvent(TraceEvent):

    """Trace event which records processing times associated with a routing event performed by the GroupFlow module.

    The following data is recorded:
    
    * igmp_trace_event: The IGMPTraceEvent associated with the IGMP packet which triggered this routing event. Note that
      not all routing events are triggered by IGMP events, and this value will be set to None in cases
      where there is no associated IGMP event. An example of this is when routing is triggered by a new
      multicast sender being detected.            
    * tree_calc_start_time: Time at which tree calculation was started for this routing event.
    * tree_calc_end_time: Time at which tree calculation was completed for this routing event.
    * route_processing_start_time: Time at which route processing was started for this routing event. Route processing is
      defined as the operation of selecting branches from the cached route tree to install for
      this particular routing event.
    * route_processing_end_time: Time at which route processing was completed for this routing event.
    * flow_installation_start_time: Time at which OpenFlow rule installation was started for this routing event.
    * flow_installation_end_time: Time at which OpenFlow rule installation was completed for this routing event.
    * multicast_group: Multicast group address which this routing event is associated with.
    * src_ip: Multicast sender IP address which this routing event is associated with.
    
    Note: Due to the tree caching behaviour of the GroupFlow module, not all routing events will require a full
    tree calculation, and in these cases tree_calc_start_time and tree_calc_end_time will be set to None
    """

    def __init__(self, event_id, igmp_trace_event=None):
        """Initializes a new GroupFlowTraceEvent with the specified event_id associated IGMPTraceEvent (defaults to None)"""
        TraceEvent.__init__(self, event_id)
        self.igmp_trace_event = igmp_trace_event    # This can be safely set to None for GroupFlow events
        # which were not triggered by an IGMP event.

        # Note: Many of these events will not require a tree recalculation, and tree_calc times will be left as None
        self.tree_calc_start_time = None
        self.tree_calc_end_time = None
        self._complete_tree_calc = False

        self.route_processing_start_time = None
        self.route_proessing_end_time = None
        self._complete_route_processing = False

        self.flow_installation_start_time = None
        self.flow_installation_end_time = None
        self._complete_flow_installation = False

        self.multicast_group = None
        self.src_ip = None

    def set_tree_calc_start_time(self, multicast_group, src_ip):
        """Records the current time as the time at which tree calculation was initiated."""
        self.multicast_group = multicast_group
        self.src_ip = src_ip
        self.tree_calc_start_time = self.get_curr_time()

    def set_tree_calc_end_time(self):
        """Records the current time as the time at which tree calculation was completed."""
        self.tree_calc_end_time = self.get_curr_time()
        self._complete_tree_calc = True

    def set_route_processing_start_time(self, multicast_group, src_ip):
        """Records the current time as the time at which route selection was initiated."""
        self.multicast_group = multicast_group
        self.src_ip = src_ip
        self.route_processing_start_time = self.get_curr_time()

    def set_route_processing_end_time(self):
        """Records the current time as the time at which route selection was completed."""
        self.route_processing_end_time = self.get_curr_time()
        self._complete_route_processing = True

    def set_flow_installation_start_time(self):
        """Records the current time as the time at which OpenFlow rule installation was initiated."""
        self.flow_installation_start_time = self.get_curr_time()

    def set_flow_installation_end_time(self):
        """Records the current time as the time at which OpenFlow rule installation was completed."""
        self.flow_installation_end_time = self.get_curr_time()
        self._complete_flow_installation = True

    def get_tree_calc_time(self):
        """Returns the length of time (in seconds) associated with tree calculation for this routing event.

        Returns None if no tree calculation was required for this routing event (often the case when the tree is
        already cached), or if the associated times have not been recorded.
        """
        if not self._complete_tree_calc:
            return None
        return self.tree_calc_end_time - self.tree_calc_start_time

    def get_route_processing_time(self):
        """Returns the length of time (in seconds) associated with route selection for this routing event.

        Returns None if the associated times have not been recorded.
        """
        if not self._complete_route_processing:
            return None

        return self.route_processing_end_time - self.route_processing_start_time

    def get_flow_installation_time(self):
        """Returns the length of time (in seconds) associated with OpenFlow rule installation for this routing event.

        Returns None if the associated times have not been recorded.
        """
        if not self._complete_flow_installation:
            return None

        return self.flow_installation_end_time - self.flow_installation_start_time

    def get_log_str(self):
        """Returns a plain-text representation of the event that will be used when the event is serialized to a log file."""
        return_string = str(self.event_id) + ' - ' + '{:10.8f}'.format(self.init_time) + '\n'
        return_string += 'Mcast Group: ' + str(self.multicast_group) + ' Src IP: ' + str(self.src_ip) + '\n'
        if self._complete_tree_calc:
            return_string += 'Tree calc time: ' + '{:10.8f}'.format(self.get_tree_calc_time() * 1000) + ' ms\n'
        if self._complete_route_processing:
            return_string += 'Route processing time: ' + '{:10.8f}'.format(
                self.get_route_processing_time() * 1000) + ' ms\n'
        if self._complete_flow_installation:
            return_string += 'Flow installation time: ' + '{:10.8f}'.format(
                self.get_flow_installation_time() * 1000) + ' ms\n'

        if not self.igmp_trace_event is None:
            return_string += 'Triggered by event:\n'
            return_string += self.igmp_trace_event.get_log_str()

        return return_string


class GroupFlowEventTracer(EventMixin):
    """The GroupFlowEventTracer is responsible for managing currently active IGMPTraceEvents and GroupFlowTraceEvents"""
    _core_name = "groupflow_event_tracer"

    def __init__(self):
        """Initializes the module once dependencies have initialized"""

        def startup():
            self._log_file_name = datetime.datetime.now().strftime("eventtrace_%H-%M-%S_%B-%d_%Y.txt")
            log.info('Writing event trace info to file: ' + str(self._log_file_name))
            self._log_file = open(self._log_file_name, 'w') # TODO: Figure out how to properly close this on shutdown
            self._module_init_time = time.time()

        self._module_init_time = 0
        self._log_file = None
        self._log_file_name = None
        self._next_event_id = 0

        self._igmp_trace_events = []
        self._groupflow_trace_events = []

        # Setup listeners
        core.call_when_ready(startup, ('openflow'))

    def termination_handler(self, signal, frame):
        """Method to cleanly terminate the module when a SIGINT signal is received.

        All currently active trace events will be discarded, and the log file will be closed. This function is
        typically called by the BenchmarkTerminator module.
        """
        if not self._log_file is None:
            self._log_file.close()
            self._log_file = None
            log.info('Termination signalled, closed log file: ' + str(self._log_file_name))

    def init_igmp_event_trace(self, router_dpid):
        """Returns a new IGMPTraceEvent with a unique event_id."""
        igmp_trace_event = IGMPTraceEvent(self._next_event_id, router_dpid)
        log.debug('Initialized IGMPTraceEvent with id: ' + str(self._next_event_id))
        self._igmp_trace_events.append(igmp_trace_event)
        self._next_event_id = self._next_event_id + 1
        return igmp_trace_event

    def init_groupflow_event_trace(self, igmp_trace_event=None):
        """Returns a new GroupFlowTraceEvent with a unique event_id."""
        groupflow_trace_event = GroupFlowTraceEvent(self._next_event_id, igmp_trace_event)
        if igmp_trace_event is None:
            log.debug('Initialized GroupFlowTraceEvent with id: ' + str(self._next_event_id))
        else:
            log.debug(
                'Initialized GroupFlowTraceEvent with id: ' + str(self._next_event_id) + ' (triggered by event: ' + str(
                    igmp_trace_event.event_id) + ')')
        self._groupflow_trace_events.append(groupflow_trace_event)
        self._next_event_id = self._next_event_id + 1
        return groupflow_trace_event

    def archive_trace_event(self, trace_event):
        """Archives the specified event by serializing it to the log file, and removes the event from the module's memory."""
        if trace_event is None:
            log.warn('Warning: Attempted to archive empty trace')
            return

        if self._log_file is None:
            return

        self._log_file.write(trace_event.get_log_str())
        self._log_file.write('\n')

        if isinstance(trace_event, IGMPTraceEvent):
            self._igmp_trace_events.remove(trace_event)
        elif isinstance(trace_event, GroupFlowTraceEvent):
            self._groupflow_trace_events.remove(trace_event)


def launch():
    # Method called by the POX core when launching the module
    core.registerNew(GroupFlowEventTracer)
