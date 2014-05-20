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
The purpose of this module is to capture a SIGINT signal, and cleanly terminate the benchmarking functionality
of the FlowTracker and GroupFlowEventTracer modules, before rethrowing the signal so that POX can catch it and terminate
entirely.

This module does not support any command line arguments.

Depends on misc.groupflow_event_tracer, openflow.flow_tracker

Created on Nov 4, 2013

Author: Alexander Craig - alexcraig1@gmail.com
"""

import time
import signal
import os
from pox.core import core
from pox.lib.revent import *

log = core.getLogger()

class BenchmarkTerminator(EventMixin):
    _core_name = "benchmark_terminator"

    def __init__(self):
        """Configures the terminate_benchmarking() method as a handler for SIGINT signals"""

        def startup():
            log.info('Module initialized.')
            self._module_init_time = time.time()

        self._module_init_time = 0
        signal.signal(signal.SIGINT, self.terminate_benchmarking)
        # Setup listeners
        core.call_when_ready(startup, ('openflow', 'openflow_flow_tracker', 'groupflow_event_tracer'))

    def terminate_benchmarking(self, signal, frame):
        """Terminates the FlowTracker and GroupFlowEventTracer modules, and rethrows the caught signal
        
        Terimation is handled by calling the termination_handler() function provided by each module.
        """
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
    # Method called by the POX core when launching the module
    core.registerNew(BenchmarkTerminator)
