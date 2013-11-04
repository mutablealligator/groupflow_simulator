#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
The only purpose of this module is to capture a SIGINT or SIGKILl signal, and terminate the benchmarking functionality
of the FlowTracker and GroupFlowEventTrace modules (before rethrowing the signal so that POX can catch it and terminate
entirely).

Depends on misc.groupflow_event_tracer and openflow.flow_tracker

Created on Nov 4, 2013
@author: alexcraig
'''

import time
import datetime
import signal
import sys
import os
from pox.core import core
from pox.lib.revent import *

log = core.getLogger()

class BenchmarkTerminator(EventMixin):
    _core_name = "benchmark_terminator"

    def __init__(self):
        # Listen to dependencies
        def startup():
            log.info('Module initialized.')
            self._module_init_time = time.time()

        self._module_init_time = 0

        signal.signal(signal.SIGINT, self.termination_handler)
        # signal.signal(signal.SIGKILL, self.termination_handler)
        
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_flow_tracker', 'groupflow_event_tracer'))
    
    def termination_handler(self, signal, frame):
        log.info('Terminating flow tracker module.')
        core.openflow_flow_tracker.termination_handler(signal, frame)
        log.info('Terminated flow tracker module.')
        log.info('Terminating groupflow event tracer module.')
        core.groupflow_event_tracer.termination_handler(signal, frame)
        log.info('Terminated groupflow event tracer module.')
        
        # Remove this signal handler, and throw a new signal that will be caught by POX
        signal.signal(signal, signal.SIG_DFL)
        os.kill(os.getpid(), signal)
    
    
def launch():
    core.registerNew(BenchmarkTerminator)
