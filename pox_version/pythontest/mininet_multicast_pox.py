from mininet.net import *
from mininet.topo import *
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.cli import CLI
from mininet.node import RemoteController
import sys
from time import sleep

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

def mcastTest(topo):
    # External controller
    # ./pox.py samples.pretty_log openflow.discovery openflow.igmp_manager openflow.groupflow log.level --WARNING --openflow.igmp_manager=WARNING --openflow.groupflow=DEBUG
    net = Mininet(topo, controller=RemoteController, link=TCLink, build=False)
    pox = RemoteController('pox', '127.0.0.1', 6633)
    net.addController('c0', RemoteController, ip = '127.0.0.1', port = 6633)
    
    net.start()
    topo.mcastConfig(net)
    # net.get('h2').cmd('python ./multicast_receiver.py &');
    # net.get('h3').cmd('python ./multicast_receiver.py &');
    # net.get('h4').cmd('python ./multicast_receiver.py &');
    # sleep(8)   # Allow time for the controller to detect the topology
    # net.get('h6').cmd('python ./multicast_receiver.py &');
    # sleep(2)
    # net.get('h1').cmd('python ./multicast_sender.py &');
    # sleep(5)
    # net.get('h5').cmd('python ./ss_multicast_receiver.py &');
    CLI(net)
    net.stop()

topos = { 'mcast_test': ( lambda: MulticastTestTopo() ) }

if __name__ == '__main__':
    setLogLevel( 'info' )
    if len(sys.argv) >= 2:
        print 'Launching BRITE defined multicast test topology'
        topo = BriteTopo(sys.argv[1])
        mcastTest(topo)
    else:
        print 'Launching default multicast test topology'
        mcastTest(MulticastTestTopo())
