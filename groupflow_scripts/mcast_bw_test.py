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

def iperf_mcast(net, hosts=None, udpBw='10M'):
    """Run iperf between two hosts using a multicast group.
       hosts: list containing the pair of hosts to test
    """
    # Validate input parameters and presence of telnet
    if not quietRun( 'which telnet' ):
        error( 'Cannot find telnet in $PATH - required for iperf test' )
        return
    if not hosts:
        hosts = [ net.hosts[ 0 ], net.hosts[ -1 ] ]
    else:
        assert len( hosts ) == 2
        
    # Configure iperf parameters
    client, server = hosts
    output( '*** Iperf: Testing UDP bandwidth between ' )
    output( "%s and %s\n" % ( client.name, server.name ) )
    output( '*** Iperf: Sending UDP bandwidth: %s\n' % udpBw)
    server.cmd( 'killall -9 iperf' )
    iperfArgs = 'iperf -u '
    bwArgs = '-b ' + udpBw + ' '

    # Launch the server
    server.sendCmd( iperfArgs + '-s -B 224.1.1.1 -i 1 ', printPid=True )
    while server.lastPid is None:
        server.monitor()
    
    # Launch the client
    output( '*** Iperf: Running test client\n' )
    cliout = client.cmd( iperfArgs + '-t 30 -c 224.1.1.1 ' +
                         bwArgs )
    output( '*** Iperf: Test client complete\n' )

    # Send two interrupts to terminate iperf 
    # First interrupt causes "Waiting for server threads to complete. Interrupt again to force quit."
    server.sendInt()
    sleep(0.5)
    server.sendInt()
    server.waitOutput(True)


class BandwidthTestTopo( Topo ):
    def __init__( self ):
        # Initialize topology
        Topo.__init__( self )
        
        # Add hosts and switches
        h0 = self.addHost('h0', ip='10.0.0.1')
        h1 = self.addHost('h1', ip='10.0.0.2')
        s0 = self.addSwitch('s0')
        s1 = self.addSwitch('s1')
        
        # Add links        
        self.addLink(s0, h0, bw = 1000, use_htb = True)
        self.addLink(s1, h1, bw = 1000, use_htb = True)
        self.addLink(s0, s1, bw = 5, use_htb = True)

    def mcastConfig(self, net):
        # Configure hosts for multicast support
        net.get('h0').cmd('route add -net 224.0.0.0/4 h0-eth0')
        net.get('h1').cmd('route add -net 224.0.0.0/4 h1-eth0')
    
    def get_host_list(self):
        return ['h0', 'h1']
    
    def get_switch_list(self):
        return ['s0', 's1']


def flowtrackerTest(topo, hosts = [], interactive = False, util_link_weight = 10, link_weight_type = 'linear'):
    # Launch the external controller
    pox_arguments = ['pox.py', 'log', '--file=pox.log,w', 'openflow.discovery',
            'openflow.flow_tracker', '--query_interval=1', '--link_max_bw=4.7', '--link_cong_threshold=2.5', '--avg_smooth_factor=0.65', '--log_peak_usage=True',
            'misc.benchmark_terminator', 'openflow.igmp_manager', 
            'openflow.groupflow', '--util_link_weight=' + str(util_link_weight), '--link_weight_type=' + link_weight_type,
            'log.level', '--WARNING', '--openflow.flow_tracker=INFO', '--openflow.igmp_manager=DEBUG']
    print 'Launching external controller: ' + str(pox_arguments[0])
    print 'Launch arguments:'
    print ' '.join(pox_arguments)
    with open(os.devnull, "w") as fnull:
        pox_process = Popen(pox_arguments, stdout=fnull, stderr=fnull, shell=False, close_fds=True)
        sleep(1)
    
    # Configure network to use external controller
    net = Mininet(topo, controller=RemoteController, switch=OVSSwitch, link=TCLink, build=False, autoSetMacs=True)
    net.addController('pox', RemoteController, ip = '127.0.0.1', port = 6633)
    net.start()
    
    # Configure control interface on network switches
    for switch_name in topo.get_switch_list():
        net.get(switch_name).controlIntf = net.get(switch_name).intf('lo')
        net.get(switch_name).cmd('route add -host 127.0.0.1 dev lo')
        net.get('pox').cmd('route add -host ' + net.get(switch_name).IP() + ' dev lo')
    
    # Setup multicast routes to virtual interfaces
    topo.mcastConfig(net)
    
    #print 'Network configuration:'
    #print net.get('h0').cmd('ifconfig')
    #print net.get('h0').cmd('route')
    #print net.get('h1').cmd('ifconfig')
    #print net.get('h1').cmd('route')
    
    sleep_time = 10
    print 'Waiting ' + str(sleep_time) + ' seconds to allow for controller topology discovery'
    sleep(sleep_time)   # Allow time for the controller to detect the topology
    
    if interactive:
        CLI(net)
    else:
        iperf_mcast(net, [net.get('h0'), net.get('h1')], udpBw='3M')
        sleep(10)
        iperf_mcast(net, [net.get('h1'), net.get('h0')], udpBw='3M')
        sleep(2)
    
    print 'Terminating controller'
    pox_process.send_signal(signal.SIGINT)
    sleep(3)
    print 'Waiting for controller termination...'
    pox_process.send_signal(signal.SIGKILL)
    pox_process.wait()
    print 'Controller terminated'
    pox_process = None
    net.stop()


if __name__ == '__main__':
    setLogLevel( 'info' )
    # Generate the test topology and run the throughput test
    topo = BandwidthTestTopo()
    hosts = topo.get_host_list()
    flowtrackerTest(topo, hosts, False)
    # Make extra sure the network terminated cleanly
    call(['python', 'kill_running_test.py'])