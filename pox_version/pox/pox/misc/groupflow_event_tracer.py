#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
This module allows event traces to be produced by the IGMP Manager and Groupflow modules, with the goal of
tracing various event processing times for benchmarking and evaluation purposes.

Created on Oct 28th, 2013
@author: alexcraig
'''

import time
import datetime
import signal
import sys
import os
from pox.core import core
from pox.lib.revent import *
from pox.lib.util import dpid_to_str
import pox.lib.packet as pkt

USE_TIME_TIME = 0   # Benchmark using time.time()
USE_TIME_CLOCK = 1  # Benchmark using time.clock()

# Set this constant to one of the above options
# time.clock() is the default if no valid option is set
TIMING_MODE = USE_TIME_TIME

log = core.getLogger()

class TraceEvent(EventMixin):
    def __init__(self, event_id = 0):
        self.init_time = self.get_curr_time()
        self.event_id = event_id
        
    def get_curr_time(self):
        global TIMING_MODE
        if TIMING_MODE == USE_TIME_TIME:
            return time.time()
        if TIMING_MODE == USE_TIME_CLOCK:
            return time.clock()
        
        # time.clock() is the default behaviour if no valid option is specified
        return time.clock()


class IGMPTraceEvent(TraceEvent):
    def __init__(self, event_id, router_dpid):
        TraceEvent.__init__(self, event_id)
        self.router_dpid = router_dpid
        self.igmp_msg_type = None
        self.igmp_group_records = []   # List of (event type, multicast group) tuples
        self.num_igmp_group_records = 0
        self.igmp_processing_start_time = 0
        self.igmp_proecssing_end_time = 0
        self._processing_complete = False
    
    def set_igmp_start_time(self, igmp_packet_in_event):
        igmp_pkt = igmp_packet_in_event.parsed.find(pkt.igmpv3)
        self.igmp_msg_type = igmp_pkt.msg_type
        for igmp_group_record in igmp_pkt.group_records:
            self.igmp_group_records.append((igmp_group_record.record_type ,igmp_group_record.multicast_address))
        self.num_igmp_group_records = len(igmp_pkt.group_records)
        self.igmp_processing_start_time = self.get_curr_time()
    
    def set_igmp_end_time(self):
        self.igmp_processing_end_time = self.get_curr_time()
        self._processing_complete = True
    
    def get_igmp_processing_time(self):
        if not self._processing_complete:
            return None
            
        return self.igmp_processing_end_time - self.igmp_processing_start_time
    
    def get_log_str(self):
        return_string = str(self.event_id) + ' - ' + '{:10.8f}'.format(self.init_time) + '\n'
        return_string += 'Router: ' + dpid_to_str(self.router_dpid) + ' IGMP Msg type: ' + str(self.igmp_msg_type) + ' Num Records: ' + str(self.num_igmp_group_records) + '\n'
        if self._processing_complete:
            return_string += 'IGMP processing time: ' + '{:10.8f}'.format(self.get_igmp_processing_time() * 1000) + ' ms\n'
        return return_string


class GroupFlowTraceEvent(TraceEvent):
    def __init__(self, event_id, igmp_trace_event = None):
        TraceEvent.__init__(self, event_id)
        self.igmp_trace_event = igmp_trace_event    # This can be safely set to None for groupflow events
                                                    # which were not triggered by an IGMP event
        
        self.tree_calc_start_time = None    # Note: Many of these events will not require a tree recalculation, and these will be left as None
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
        self.multicast_group = multicast_group
        self.src_ip = src_ip
        self.tree_calc_start_time = self.get_curr_time()
        
    def set_tree_calc_end_time(self):
        self.tree_calc_end_time = self.get_curr_time()
        self._complete_tree_calc = True
    
    def set_route_processing_start_time(self, multicast_group, src_ip):
        self.multicast_group = multicast_group
        self.src_ip = src_ip
        self.route_processing_start_time = self.get_curr_time()
    
    def set_route_processing_end_time(self):
        self.route_processing_end_time = self.get_curr_time()
        self._complete_route_processing = True
    
    def set_flow_installation_start_time(self):
        self.flow_installation_start_time = self.get_curr_time()
    
    def set_flow_installation_end_time(self):
        self.flow_installation_end_time = self.get_curr_time()
        self._complete_flow_installation = True
    
    def get_tree_calc_time(self):
        if not self._complete_tree_calc:
            return None
        
        return self.tree_calc_end_time - self.tree_calc_start_time
    
    def get_route_processing_time(self):
        if not self._complete_route_processing:
            return None
        
        return self.route_processing_end_time - self.route_processing_start_time
    
    def get_flow_installation_time(self):
        if not self._complete_flow_installation:
            return None
        
        return self.flow_installation_end_time - self.flow_installation_start_time
    
    def get_log_str(self):
        return_string = str(self.event_id) + ' - ' + '{:10.8f}'.format(self.init_time) + '\n'
        return_string += 'Mcast Group: ' + str(self.multicast_group) + ' Src IP: ' + str(self.src_ip) + '\n'
        if self._complete_tree_calc:
            return_string += 'Tree calc time: ' + '{:10.8f}'.format(self.get_tree_calc_time() * 1000) + ' ms\n'
        if self._complete_route_processing:
            return_string += 'Route processing time: ' + '{:10.8f}'.format(self.get_route_processing_time() * 1000) + ' ms\n'
        if self._complete_flow_installation:
            return_string += 'Flow installation time: ' + '{:10.8f}'.format(self.get_flow_installation_time() * 1000) + ' ms\n'
        
        if not self.igmp_trace_event is None:
            return_string += 'Triggered by event:\n'
            return_string += self.igmp_trace_event.get_log_str()
            
        return return_string

        
class GroupFlowEventTracer(EventMixin):
    _core_name = "groupflow_event_tracer"

    def __init__(self):
        # Listen to dependencies
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
        
        signal.signal(signal.SIGINT, self.termination_handler)
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow'))
    
    def termination_handler(self, signal, frame):
        if not self._log_file is None:
            self._log_file.close()
            self._log_file = None
            log.info('Termination signalled, closed log file: ' + str(self._log_file_name))
            signal.signal(signal, signal.SIG_DFL)
            os.kill(os.getpid(), signal)
    
    def init_igmp_event_trace(self, router_dpid):
        igmp_trace_event = IGMPTraceEvent(self._next_event_id, router_dpid)
        log.debug('Initialized IGMPTraceEvent with id: ' + str(self._next_event_id))
        self._igmp_trace_events.append(igmp_trace_event)
        self._next_event_id = self._next_event_id + 1
        return igmp_trace_event
    
    def init_groupflow_event_trace(self, igmp_trace_event = None):
        groupflow_trace_event = GroupFlowTraceEvent(self._next_event_id, igmp_trace_event)
        if igmp_trace_event is None:
            log.debug('Initialized GroupFlowTraceEvent with id: ' + str(self._next_event_id))
        else:
            log.debug('Initialized GroupFlowTraceEvent with id: ' + str(self._next_event_id) + ' (triggered by event: ' + str(igmp_trace_event.event_id) + ')')
        self._groupflow_trace_events.append(groupflow_trace_event)
        self._next_event_id = self._next_event_id + 1
        return groupflow_trace_event
    
    def archive_trace_event(self, trace_event):
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
    core.registerNew(GroupFlowEventTracer)
