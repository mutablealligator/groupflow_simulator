#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
A POX module implementation of the CastFlow clean slate multicast proposal.

Implementation adapted from NOX-Classic CastFlow implementation provided by caioviel.

Created on July 16, 2013
@author: alexcraig
'''

from collections import defaultdict
from heapq import heapify, heappop, heappush

from pox.openflow.discovery import Discovery
from pox.core import core

log = core.getLogger()


class CastflowManager(object):

    def __init__(self):
        core.openflow_discovery.addListeners(self)

    def _handle_LinkEvent(self, event):
        log.debug('Handling LinkEvent: ' + str(event.link) + ' - State: ' + str(event.added))


def launch():
    core.registerNew(CastflowManager)