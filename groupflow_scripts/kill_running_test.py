#!/usr/bin/env python
import os
import sys
import signal
from subprocess import call

ps_out = os.popen('ps -e | grep python2.7')
for line in ps_out:
    print line,
    line_split = line.strip().split(' ')
    os.kill(int(line_split[0]), signal.SIGTERM)

ps_out = os.popen('ps -e | grep vlc')
for line in ps_out:
    print line,
    line_split = line.strip().split(' ')
    os.kill(int(line_split[0]), signal.SIGTERM)

ps_out = os.popen('ps -e | grep controller')
for line in ps_out:
    print line,
    line_split = line.strip().split(' ')
    os.kill(int(line_split[0]), signal.SIGTERM)

ps_out = os.popen('ps -e | grep python')
for line in ps_out:
    line_split = line.strip().split(' ')
    proc_id = int(line_split[0])
    if proc_id != os.getpid() and proc_id != os.getppid():
        print line,
        os.kill(proc_id, signal.SIGTERM)

call(['mn', '-c'])