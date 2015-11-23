#!/usr/bin/env python
import os
import sys
import signal
from subprocess import call

call('rm -rfv /usr/local/home/cse222a05/GroupFlow/groupflow_scripts/msglist', shell=True)
call('rm -rf /usr/local/home/cse222a05/GroupFlow/groupflow_scripts/output/*', shell=True)

ps_out = os.popen('ps -e | grep python2.7')
for line in ps_out:
    print line,
    line_split = line.strip().split(' ')
    try:
        os.kill(int(line_split[0]), signal.SIGTERM)
    except Exception as e:
        print str(e.strerror)

ps_out = os.popen('ps -e | grep vlc')
for line in ps_out:
    print line,
    line_split = line.strip().split(' ')
    try:
        os.kill(int(line_split[0]), signal.SIGTERM)
    except Exception as e:
        print str(e.strerror)

ps_out = os.popen('ps -e | grep controller')
for line in ps_out:
    print line,
    line_split = line.strip().split(' ')
    try:
        os.kill(int(line_split[0]), signal.SIGTERM)
    except Exception as e:
        print str(e.strerror)

ps_out = os.popen('ps -e | grep python')
for line in ps_out:
    line_split = line.strip().split(' ')
    proc_id = int(line_split[0])
    if proc_id != os.getpid() and proc_id != os.getppid():
        print line,
        try:
            os.kill(proc_id, signal.SIGTERM)
        except Exception as e:
            print str(e.strerror)

call(['mn', '-c'])
call(['rm', '-rf', 'mcastlog_*'])
