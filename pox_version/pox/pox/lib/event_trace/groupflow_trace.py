#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
This file defines events which are used by the IGMP Manager and Groupflow modules to provide
traces of event processing times for benchmarking and evaluation purposes.

Created on Oct 28th, 2013
@author: alexcraig
'''

import time
from pox.lib.revent import *
from pox.lib.util import dpid_to_str
import pox.lib.packet as pkt

USE_TIME_TIME = 0   # Benchmark using time.time()
USE_TIME_CLOCK = 1  # Benchmark using time.clock()

# Set this constant to one of the above options
# time.clock() is the default if no valid option is set
TIMING_MODE = USE_TIME_TIME

class TraceEvent(EventMixin):
    def __init__(self):
        self.init_time = self.get_curr_time()
        
    def get_curr_time(self):
        global TIMING_MODE
        if TIMING_MODE == USE_TIME_TIME:
            return time.time()
        if TIMING_MODE == USE_TIME_CLOCK:
            return time.clock()
        
        # time.clock() is the default behaviour if no valid option is specified
        return time.clock()


class IGMPTraceEvent(TraceEvent):
    def __init__(self, router_dpid):
        self.router_dpid = router_dpid
        self.igmp_msg_type = None
        self.igmp_group_records = []   # List of (event type, multicast group) tuples
        self.num_igmp_group_records = 0
        self.igmp_processing_start_time = 0
        self.igmp_proecssing_end_time = 0
        self._processing_complete = False
    
    def set_igmp_start_time(self, igmp_packet_in_event):
        igmp_pkt = igmp_packet_in_event.parsed.find(pkt.igmp)
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
    
    def debug_print(self, log):
        log.debug('Router: ' + dpid_to_str(self.router_dpid) + ' IGMP Msg type: ' + str(self.igmp_msg_type) + ' Num Records: ' + str(self.num_igmp_group_records))
        if self._processing_complete:
            log.debug('IGMP processing time: ' + '{:10.8f}'.format(self.get_igmp_processing_time() * 1000) + ' ms')


class GroupFlowTraceEvent(TraceEvent):
    def __init__(self, igmp_trace_event = None):
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
    
    def debug_print(self, log):
        log.debug(' ')
        log.debug('Mcast Group: ' + str(self.multicast_group) + ' Src IP: ' + str(self.src_ip))
        if self._complete_tree_calc:
            log.debug('Tree calc time: ' + '{:10.8f}'.format(self.get_tree_calc_time() * 1000) + ' ms')
        if self._complete_route_processing:
            log.debug('Route processing time: ' + '{:10.8f}'.format(self.get_route_processing_time() * 1000) + ' ms')
        if self._complete_flow_installation:
            log.debug('Flow installation time: ' + '{:10.8f}'.format(self.get_flow_installation_time() * 1000) + ' ms')
        
        if not self.igmp_trace_event is None:
            log.debug('Triggered by event:')
            igmp_trace_event.debug_print(log)
        log.debug(' ')