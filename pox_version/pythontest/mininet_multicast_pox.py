from mininet.net import *
from mininet.topo import *
from mininet.node import OVSSwitch
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

HOST_MACHINE_IP = '192.168.198.129'

class MulticastGroupDefinition(object):
    def __init__(self, src_host, dst_hosts, group_ip, mcast_port, echo_port):
        self.src_host = src_host
        self.dst_hosts = dst_hosts
        self.group_ip = group_ip
        self.mcast_port = mcast_port
        self.echo_port = echo_port
        
        self.src_process = None
        self.dst_processes = []
    
    def launch_mcast_applications(self, net):
        # print 'Initializing multicast group ' + str(self.group_ip) + ':' + str(self.mcast_port) + ' Echo port: ' + str(self.echo_port)
        sender_shell_command = 'python ./multicast_sender.py {group_ip} {mcast_port} {echo_port}'
        sender_shell_command = sender_shell_command.format(group_ip = self.group_ip, mcast_port = str(self.mcast_port), echo_port = str(self.echo_port))
        # print 'Sender shell command: ' + str(sender_shell_command)
        with open(os.devnull, "w") as fnull:
            self.src_process = net.get(self.src_host).popen(['python', './multicast_sender.py', self.group_ip, str(self.mcast_port), str(self.echo_port)], stdout=fnull, stderr=fnull, close_fds=True)
        
        for dst in self.dst_hosts:
            dst_shell_command = 'python ./multicast_receiver.py {group_ip} {mcast_port} {echo_port} >/dev/null 2>&1'
            dst_shell_command = dst_shell_command.format(group_ip = self.group_ip, mcast_port = str(self.mcast_port), echo_port = str(self.echo_port))
            # print 'Receiver shell command: ' + str(dst_shell_command)
            with open(os.devnull, "w") as fnull:
                self.dst_processes.append(net.get(dst).popen(['python', './multicast_receiver.py', self.group_ip, str(self.mcast_port), str(self.echo_port)], stdout=fnull, stderr=fnull, close_fds=True))
        
        print('Initialized multicast group ' + str(self.group_ip) + ':' + str(self.mcast_port)
                + ' Echo port: ' + str(self.echo_port) + ' # Receivers: ' + str(len(self.dst_processes)))
    
    def terminate_mcast_applications(self):
        if self.src_process is not None:
            print 'Killing process with PID: ' + str(self.src_process.pid)
            self.src_process.send_signal(signal.SIGTERM)
            self.src_process = None
            
        for proc in self.dst_processes:
            print 'Killing process with PID: ' + str(proc.pid)
            proc.send_signal(signal.SIGTERM)
        self.dst_processes = []
        
        print 'Terminated multicast group ' + str(self.group_ip) + ':' + str(self.mcast_port) + ' Echo port: ' + str(self.echo_port)

def generate_group_membership_probabilities(hosts, mean, std_dev, avg_group_size = 0):
    num_hosts = len(hosts)
    a , b = a, b = (0 - mean) / std_dev, (1 - mean) / std_dev
    midpoint_ab = (b + a) / 2
    scale = 1 / (b - a)
    location = 0.5 - (midpoint_ab * scale)
    rv = truncnorm(a, b, loc=location, scale=scale)
    rvs = rv.rvs(num_hosts)
    if avg_group_size > 0:
        rvs_sum = sum(rvs)
        rvs = [p / (rvs_sum/float(avg_group_size)) for p in rvs]
        
    prob_tuples = []
    for index, host in enumerate(hosts):
        prob_tuples.append((host, rvs[index]))
    
    return prob_tuples
    
        
def connectToRootNS( network, switch, ip, prefixLen, routes ):
    """Connect hosts to root namespace via switch. Starts network.
      network: Mininet() network object
      switch: switch to connect to root namespace
      ip: IP address for root namespace node
      prefixLen: IP address prefix length (e.g. 8, 16, 24)
      routes: host networks to route to"""
    # Create a node in root namespace and link to switch 0
    root = Node( 'root', inNamespace=False )
    intf = Link( root, switch ).intf1
    root.setIP( ip, prefixLen, intf )
    # Start network that now includes link to root namespace
    network.start()
    # Add routes from root ns to hosts
    for route in routes:
        root.cmd( 'route add -net ' + route + ' dev ' + str( intf ) )

class BriteTopo(Topo):
    def __init__(self, brite_filepath):
        # Initialize topology
        Topo.__init__( self )
        
        self.hostnames = []
        self.routers = []
        
        print 'Parsing BRITE topology at filepath: ' + str(brite_filepath)
        file = open(brite_filepath, 'r')
        line = file.readline()
        print 'BRITE ' + line
        
        # Skip ahead until the nodes section is reached
        in_node_section = False
        while not in_node_section:
            line = file.readline()
            if 'Nodes:' in line:
                in_node_section = True
                break
        
        # In the nodes section now, generate a switch and host for each node
        while in_node_section:
            line = file.readline().strip()
            if not line:
                in_node_section = False
                print 'Finished parsing nodes'
                break
            
            line_split = line.split('\t')
            node_id = int(line_split[0])
            print 'Generating switch and host for ID: ' + str(node_id)
            switch = self.addSwitch('s' + str(node_id))
            host = self.addHost('h' + str(node_id))
            self.addLink(switch, host, bw=10, use_htb=True)	# TODO: Better define link parameters for hosts
            self.routers.append(switch)
            self.hostnames.append('h' + str(node_id))
            
        # Skip ahead to the edges section
        in_edge_section = False
        while not in_edge_section:
            line = file.readline()
            if 'Edges:' in line:
                in_edge_section = True
                break
        
        # In the edges section now, add all required links
        while in_edge_section:
            line = file.readline().strip()
            if not line:    # Empty string
                in_edge_section = False
                print 'Finished parsing edges'
                break
                
            line_split = line.split('\t')
            switch_id_1 = int(line_split[1])
            switch_id_2 = int(line_split[2])
            delay_ms = str(float(line_split[4])) + 'ms'
            bandwidth_Mbps = float(line_split[5])
            print 'Adding link between switch ' + str(switch_id_1) + ' and ' + str(switch_id_2) + '\n\tRate: ' \
                + str(bandwidth_Mbps) + ' Mbps\tDelay: ' + delay_ms
            # params = {'bw':bandwidth_Mbps, 'delay':delay_ms}]
            # TODO: Figure out why setting the delay won't work
            self.addLink(self.routers[switch_id_1], self.routers[switch_id_2], bw=bandwidth_Mbps, delay=delay_ms, use_htb=True)
        
        file.close()
    
    def get_host_list(self):
        return self.hostnames
    
    def mcastConfig(self, net):
        for hostname in self.hostnames:
            net.get(hostname).cmd('route add -net 224.0.0.0/4 ' + hostname + '-eth0')
        
class MulticastTestTopo( Topo ):
    "Simple multicast testing example"
    
    def __init__( self ):
        "Create custom topo."
        
        # Initialize topology
        Topo.__init__( self )
        
        # Add hosts and switches
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')
        h3 = self.addHost('h3')
        h4 = self.addHost('h4')
        h5 = self.addHost('h5')
        h6 = self.addHost('h6')
        h7 = self.addHost('h7')
        h8 = self.addHost('h8')
        h9 = self.addHost('h9')
        h10 = self.addHost('h10')
        h11 = self.addHost('h11')
        
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')
        s5 = self.addSwitch('s5')
        s6 = self.addSwitch('s6')
        s7 = self.addSwitch('s7')
        
        
        # Add links
        self.addLink(s1, s2, bw = 10, use_htb = True)
        self.addLink(s1, s3, bw = 10, use_htb = True)
        self.addLink(s2, s4, bw = 10, use_htb = True)
        self.addLink(s4, s5, bw = 10, use_htb = True)
        self.addLink(s2, s5, bw = 10, use_htb = True)
        self.addLink(s2, s6, bw = 10, use_htb = True)
        self.addLink(s6, s3, bw = 10, use_htb = True)
        self.addLink(s3, s7, bw = 10, use_htb = True)
        self.addLink(s7, s5, bw = 10, use_htb = True)
        
        self.addLink(s2, h1, bw = 10, use_htb = True)
        self.addLink(s3, h2, bw = 10, use_htb = True)
        self.addLink(s3, h3, bw = 10, use_htb = True)
        self.addLink(s5, h4, bw = 10, use_htb = True)
        self.addLink(s5, h5, bw = 10, use_htb = True)
        self.addLink(s5, h6, bw = 10, use_htb = True)
        self.addLink(s2, h7, bw = 10, use_htb = True)
        self.addLink(s6, h8, bw = 10, use_htb = True)
        self.addLink(s7, h9, bw = 10, use_htb = True)
        self.addLink(s4, h10, bw = 10, use_htb = True)
        self.addLink(s1, h11, bw = 10, use_htb = True)

    def mcastConfig(self, net):
        # Configure hosts for multicast support
        net.get('h1').cmd('route add -net 224.0.0.0/4 h1-eth0')
        net.get('h2').cmd('route add -net 224.0.0.0/4 h2-eth0')
        net.get('h3').cmd('route add -net 224.0.0.0/4 h3-eth0')
        net.get('h4').cmd('route add -net 224.0.0.0/4 h4-eth0')
        net.get('h5').cmd('route add -net 224.0.0.0/4 h5-eth0')
        net.get('h6').cmd('route add -net 224.0.0.0/4 h6-eth0')
        net.get('h7').cmd('route add -net 224.0.0.0/4 h7-eth0')
        net.get('h8').cmd('route add -net 224.0.0.0/4 h8-eth0')
        net.get('h9').cmd('route add -net 224.0.0.0/4 h9-eth0')
        net.get('h10').cmd('route add -net 224.0.0.0/4 h10-eth0')
        net.get('h11').cmd('route add -net 224.0.0.0/4 h11-eth0')
    
    def get_host_list(self):
        return ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'h8', 'h9', 'h10', 'h11']

def mcastTest(topo, hosts = []):
    # Launch the external controller
    pox_arguments = ['pox.py', 'log', '--file=pox.log,w', 'openflow.discovery', 'openflow.flow_tracker', 'misc.benchmark_terminator', 'misc.groupflow_event_tracer', 
            'openflow.igmp_manager', 'openflow.groupflow', 'log.level', '--WARNING', '--openflow.groupflow=DEBUG']
    print 'Launching external controller: ' + str(pox_arguments[0])
    
    with open(os.devnull, "w") as fnull:
        pox_process = Popen(pox_arguments, stdout=fnull, stderr=fnull, shell=False, close_fds=True)
        # Allow time for the log file to be generated
        sleep(1)
    
    # Determine the flow tracker log file
    pox_log_file = open('./pox.log', 'r');
    flow_log_path = None
    got_flow_log_path = False
    while not got_flow_log_path:
        pox_log = pox_log_file.readline()

        if 'Writing flow tracker info to file:' in pox_log:
            pox_log_split = pox_log.split()
            flow_log_path = pox_log_split[-1]
            got_flow_log_path = True
            
    print 'Got flow tracker log file: ' + str(flow_log_path)
    print 'Controller initialized'
    
    # External controller
    # ./pox.py samples.pretty_log openflow.discovery openflow.flow_tracker misc.benchmark_terminator misc.groupflow_event_tracer openflow.igmp_manager openflow.groupflow log.level --WARNING --openflow.igmp_manager=WARNING --openflow.groupflow=DEBUG
    net = Mininet(topo, controller=RemoteController, switch=OVSSwitch, link=TCLink, build=False, autoSetMacs=True)
    pox = RemoteController('pox', '127.0.0.1', 6633)
    net.addController('c0', RemoteController, ip = '127.0.0.1', port = 6633)
    
    net.start()
    
    # Setup ssh access so that VLC can be run on hosts
    #cmd='/usr/sbin/sshd'
    #opts='-D'
    #switch = net.switches[ 0 ]  # switch to use
    #ip = HOST_MACHINE_IP  # our IP address on host network
    #routes = [ '10.0.0.0/8' ]  # host networks to route to
    #connectToRootNS( net, switch, ip, 8, routes )
    #for host in net.hosts:
    #    host.cmd( cmd + ' ' + opts + '&' )
    #print
    #print "*** Hosts are running sshd at the following addresses:"
    #print
    #for host in net.hosts:
    #    print host.name, host.IP()
    
    test_groups = []
    test_group_launch_times = []
    
    topo.mcastConfig(net)
    print 'Waiting 10 seconds to allow for controller topology discovery'
    sleep(10)   # Allow time for the controller to detect the topology
    
    mcast_group_last_octet = 1
    mcast_port = 5010
    
    host_join_probabilities = generate_group_membership_probabilities(hosts, 0.25, 0.5)
    for i in range(0,5):
        print 'Generating multicast group #' + str(i)
        # Choose a sending host using a uniform random distribution
        sender_index = randint(0,len(hosts))
        sender_host = hosts[sender_index]
        
        # Choose a random number of sender by comparing a uniform random variable
        # against the previously generated group membership probabilities
        receivers = []
        for host_prob in host_join_probabilities:
            p = uniform(0, 1)
            if p >= host_prob[1]:
                receivers.append(host_prob[0])
        
        # Initialize the group
        # Note - This method of group IP generation will need to be modified slightly to support more than
        # 255 groups
        mcast_ip = '224.1.1.{last_octet}'.format(last_octet = str(mcast_group_last_octet))
        test_groups.append(MulticastGroupDefinition(sender_host, receivers, mcast_ip, mcast_port, mcast_port + 1))
        launch_time = time()
        test_group_launch_times.append(launch_time)
        print 'Launching multicast group #' + str(i) + ' at time: ' + str(launch_time)
        test_groups[-1].launch_mcast_applications(net)
        mcast_group_last_octet = mcast_group_last_octet + 1
        mcast_port = mcast_port + 2
        sleep(5)

    CLI(net)
    
    print 'Terminating controller'
    pox_log_file.close()
    pox_process.send_signal(signal.SIGKILL)
    pox_process = None
    print 'Controller terminated'
    
    for group in test_groups:
        group.terminate_mcast_applications()
    net.stop()

topos = { 'mcast_test': ( lambda: MulticastTestTopo() ) }

if __name__ == '__main__':
    setLogLevel( 'info' )
    if len(sys.argv) >= 2:
        print 'Launching BRITE defined multicast test topology'
        topo = BriteTopo(sys.argv[1])
        hosts = topo.get_host_list()
        mcastTest(topo, hosts)
    else:
        print 'Launching default multicast test topology'
        topo = MulticastTestTopo()
        hosts = topo.get_host_list()
        mcastTest(topo, hosts)
