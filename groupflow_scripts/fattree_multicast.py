#!/usr/bin/env python
from groupflow_shared import *
from mininet.net import *
from mininet.node import OVSSwitch, UserSwitch
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.cli import CLI
from mininet.node import Node, RemoteController
from scipy.stats import truncnorm
from numpy.random import randint, uniform
from subprocess import *
import sys
import signal
from time import sleep, time
from datetime import datetime
from multiprocessing import Process
import numpy as np

class FlowTrackerTestTopo( Topo ):
    def __init__( self ):
        "Create custom topo."
        
        # Initialize topology
        Topo.__init__( self )
        
        # Add hosts and switches
        h0 = self.addHost('h0', ip='10.0.0.1')
        h1 = self.addHost('h1', ip='10.0.0.2')
	h2 = self.addHost('h2', ip='10.0.0.3')
	h3 = self.addHost('h3', ip='10.0.0.4')
	h4 = self.addHost('h4', ip='10.0.0.5')
	h5 = self.addHost('h5', ip='10.0.0.6')
        
        s0 = self.addSwitch('s0')
        s1 = self.addSwitch('s1')
	s2 = self.addSwitch('s2')
	s3 = self.addSwitch('s3')
	s4 = self.addSwitch('s4')
	s5 = self.addSwitch('s5')
        s6 = self.addSwitch('s6')
	s7 = self.addSwitch('s7')
	s8 = self.addSwitch('s8')
	s9 = self.addSwitch('s9')
	s10 = self.addSwitch('s10')
	s11 = self.addSwitch('s11')

        # Add links        
        self.addLink(s0, h0, bw = 1000, use_htb = True)
        self.addLink(s0, h1, bw = 1000, use_htb = True)
	self.addLink(s1, h2, bw = 1000, use_htb = True)

        self.addLink(s0, s2, bw = 1000, use_htb = True)
	self.addLink(s0, s3, bw = 1000, use_htb = True)
	self.addLink(s1, s2, bw = 1000, use_htb = True)
	self.addLink(s1, s3, bw = 1000, use_htb = True)
	self.addLink(s3, s5, bw = 1000, use_htb = True)
	self.addLink(s2, s4, bw = 1000, use_htb = True)
	
	self.addLink(s6, h3, bw = 1000, use_htb = True)
	self.addLink(s7, h4, bw = 1000, use_htb = True)
	self.addLink(s7, h5, bw = 1000, use_htb = True)

	self.addLink(s6, s8, bw = 1000, use_htb = True)
	self.addLink(s6, s9, bw = 1000, use_htb = True)
	self.addLink(s7, s8, bw = 1000, use_htb = True)
	self.addLink(s7, s9, bw = 1000, use_htb = True)
	self.addLink(s8, s10, bw = 1000, use_htb = True)
	self.addLink(s9, s11, bw = 1000, use_htb = True)

	self.addLink(s4, s8, bw = 1000, use_htb = True)
	self.addLink(s5, s9, bw = 1000, use_htb = True)
	self.addLink(s10, s2, bw = 1000, use_htb = True)
	self.addLink(s11, s3, bw = 1000, use_htb = True)

    def mcastConfig(self, net):
        # Configure hosts for multicast support
        net.get('h0').cmd('route add -net 224.0.0.0/4 h0-eth0')
        net.get('h1').cmd('route add -net 224.0.0.0/4 h1-eth0')
	net.get('h2').cmd('route add -net 224.0.0.0/4 h2-eth0')
	net.get('h3').cmd('route add -net 224.0.0.0/4 h3-eth0')
	net.get('h4').cmd('route add -net 224.0.0.0/4 h4-eth0')
	net.get('h5').cmd('route add -net 224.0.0.0/4 h5-eth0')
    
    def get_host_list(self):
        return ['h0', 'h1', 'h2', 'h3', 'h4', 'h5']
    
    def get_switch_list(self):
        return ['s0', 's1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9', 's10', 's11']

def flowtrackerTest(topo, hosts = [], interactive = False, util_link_weight = 10, link_weight_type = 'linear'):
    # Launch the external controller
    pox_arguments = ['/usr/local/home/cse222a05/pox/pox.py', 'log', '--file=/usr/local/home/cse222a05/GroupFlow/groupflow_scripts/pox.log,w', 'openflow.discovery', 'forwarding.l2_learning', 'misc.benchmark_terminator', 
			'openflow.igmp_manager', 'misc.groupflow_event_tracer', 'openflow.groupflow']
    print 'Launching external controller: ' + str(pox_arguments[0])
    print 'Launch arguments:'
    print ' '.join(pox_arguments)
    
    with open(os.devnull, "w") as fnull:
        pox_process = Popen(pox_arguments, stdout=fnull, stderr=fnull, shell=False, close_fds=True)
        # Allow time for the log file to be generated
        sleep(1)
    
    # External controller
    net = Mininet(topo, controller=RemoteController, switch=OVSSwitch, link=TCLink, build=False, autoSetMacs=True)
    # pox = RemoteController('pox', '127.0.0.1', 6633)
    net.addController('pox', RemoteController, ip = '127.0.0.1', port = 6633)
    net.start()
    
    #for switch_name in topo.get_switch_list():
    #    net.get(switch_name).controlIntf = net.get(switch_name).intf('lo')
    #    net.get(switch_name).cmd('route add -host 127.0.0.1 dev lo')
    #    net.get('pox').cmd('route add -host ' + net.get(switch_name).IP() + ' dev lo')
        
    topo.mcastConfig(net)
    
    print 'Network configuration:'
    for host in hosts:
    	print net.get(host).cmd('ifconfig')
    	print net.get(host).cmd('route')
    
    sleep_time = 2
    print 'Waiting ' + str(sleep_time) + ' seconds to allow for controller topology discovery'
    sleep(sleep_time)   # Allow time for the controller to detect the topology
    
    if not interactive:
        net.ping([net.get('h0'), net.get('h1')])
        net.iperf([net.get('h0'), net.get('h1')], l4Type='UDP')
        net.iperf([net.get('h1'), net.get('h0')], l4Type='UDP')
    else:
        print 'Launching test applications...'
        sender_proc = None
        sender_log = open('sender_log.txt', 'w')
        receiver_proc = None
        receiver_log = open('receiver_log.txt', 'w')
        # Launch multicast sender and receiver
        sender_command = ['python', 'multicast_sender.py']
        receiver_command = ['python', 'multicast_receiver.py']
        receiver_proc = net.get('h1').popen(' '.join(receiver_command), stdout=receiver_log, stderr=receiver_log, close_fds=True, shell=False)
        sender_proc = net.get('h3').popen(' '.join(sender_command), stdout=sender_log, stderr=sender_log, close_fds=True, shell=False)

	
        
        print 'Launched test applications'
        
        sleep(15)
        
        print 'Terminating test applications'
        sender_log.flush()
        receiver_log.flush()
        
        sender_proc.terminate()
        sender_proc.wait()
        sender_proc = None
        
        receiver_proc.terminate()
        receiver_proc.wait()
        receiver_proc = None
    
    print 'Terminating controller'
    pox_process.send_signal(signal.SIGINT)
    sleep(3)
    print 'Waiting for controller termination...'
    pox_process.send_signal(signal.SIGKILL)
    pox_process.wait()
    print 'Controller terminated'
    pox_process = None
    net.stop()

    #write_final_stats_log(log_file_name, flow_log_path, event_log_path, membership_mean, membership_std_dev, membership_avg_bound, test_groups, test_group_launch_times, topo)

topos = { 'mcast_test': ( lambda: MulticastTestTopo() ) }

if __name__ == '__main__':
    setLogLevel( 'info' )

    # Interactive mode with barebones topology
    print 'Launching default multicast test topology'
    topo = FlowTrackerTestTopo()
    hosts = topo.get_host_list()
    flowtrackerTest(topo, hosts, True)
    # Make extra sure the network terminated cleanly
    #call(['python', 'kill_running_test.py'])
