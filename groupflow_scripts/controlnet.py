#!/usr/bin/python
from mininet.net import Mininet
from mininet.node import Controller, RemoteController, UserSwitch
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from time import sleep
import os
import signal

DATA_CONTROL_IP='192.168.123.254'
DATA_CONTROL_PORT=6634
OFPP_CONTROLLER=65533

class DataController( RemoteController ):
    def checkListening( self ):
        "Ignore spurious error"
        pass

class MininetFacade( object ):
    """Mininet object facade that allows a single CLI to
       talk to one or more networks"""
    
    def __init__( self, net, *args, **kwargs ):
        """Create MininetFacade object.
           net: Primary Mininet object
           args: unnamed networks passed as arguments
           kwargs: named networks passed as arguments"""
        self.net=net
        self.nets=[ net ] + list( args ) + kwargs.values()
        self.nameToNet=kwargs
        self.nameToNet['net']=net

    def __getattr__( self, name ):
        "returns attribute from Primary Mininet object"
        return getattr( self.net, name )

    def __getitem__( self, key ):
        "returns primary/named networks or node from any net"
        #search kwargs for net named key
        if key in self.nameToNet:
            return self.nameToNet[ key ]
        #search each net for node named key
        for net in self.nets:
            if key in net:
                return net[ key ]

    def __iter__( self ):
        "Iterate through all nodes in all Mininet objects"
        for net in self.nets:
            for node in net:
                yield node

    def __len__( self ):
        "returns aggregate number of nodes in all nets"
        count=0
        for net in self.nets:
            count += len(net)
        return count

    def __contains__( self, key ):
        "returns True if node is a member of any net"
        return key in self.keys()

    def keys( self ):
        "returns a list of all node names in all networks"
        return list( self )

    def values( self ):
        "returns a list of all nodes in all networks"
        return [ self[ key ] for key in self ]

    def items( self ):
        "returns (key,value) tuple list for every node in all networks"
        return zip( self.keys(), self.values() )

class TestTopology( Topo ):
    def __init__( self, control_network=False, dpid_base=0, **kwargs ):
        Topo.__init__( self, **kwargs )
        
        # Generate test topology (star of 5 switches)
        switches=[]
        for switch_id in range(0, 5):
            dpid="%0.12X" % (dpid_base + switch_id)
            if control_network:
                switches.append(self.addSwitch('cs%s' % switch_id, inNamespace=True, dpid=dpid))
            else:
                switches.append(self.addSwitch('s%s' % switch_id, inband=True, inNamespace=True, dpid=dpid))
        
        self.addLink(switches[0], switches[1], bw=10, delay='1ms')
        self.addLink(switches[0], switches[2], bw=10, delay='1ms')
        self.addLink(switches[0], switches[3], bw=10, delay='1ms')
        self.addLink(switches[0], switches[4], bw=10, delay='1ms')
        
        if control_network:
            # Add a host which will run the controller for the data network
            pox=self.addHost('pox', cls=DataController, ip=DATA_CONTROL_IP, port=DATA_CONTROL_PORT, inNamespace=True)
            self.addLink(pox, switches[0], bw=10, delay='1ms')

def run():
    "Create control and data networks, and launch POX"
    
    # Launch the control network
    info('* Creating Control Network\n')
    ctopo=TestTopology(control_network=True, dpid_base=100)
    cnet=Mininet(topo=ctopo, ipBase='192.168.123.0/24', switch=UserSwitch, controller=None, link=TCLink, build=False, autoSetMacs=False)
    info('* Adding Control Network Controller\n')
    cnet.addController('cc0', controller=Controller)
    info('* Starting Control Network\n')
    cnet.start()
    
    # Launch the network controller for the data network
    pox_process=None
    pox_arguments=['pox.py', 'log', '--file=pox.log,w', 'openflow.of_01', '--address=%s' % DATA_CONTROL_IP, '--port=%s' % DATA_CONTROL_PORT, 'openflow.discovery', 'log.level', '--DEBUG']
    info('* Launching POX: ' + ' '.join(pox_arguments) + '\n')
    # Direct POX stdout and stderr to /dev/null
    with open(os.devnull, "w") as fnull:
        pox_process=cnet.get('pox').popen(pox_arguments, stdout=fnull, stderr=fnull, shell=False, close_fds=True)
    sleep(1)    # Arbitrary delay to allow controller to initialize
    
    # Launch the data network
    info('* Creating Data Network\n' )
    topo=TestTopology(control_network=False, dpid_base=0)
    net=Mininet(topo=topo, switch=UserSwitch, controller=None, link=TCLink, build=False, autoSetMacs=False)
    info('* Adding Data Controllers to Data Network\n')
    for host in cnet.hosts:
        if isinstance(host, DataController):
            net.addController(host)
            info('* Added DataController Running on Host "%s" to Data Network\n' % str(host))
    info('* Starting Data Network\n')
    net.start()
    info('* Connect Data Network Switch Management Interfaces to Control Network Switches\n')
    for switch1 in net.switches:
        if isinstance(switch1, UserSwitch):
            switch_id=int(str(switch1)[1:])
            switch2=cnet.get('cs%s' % switch_id)
            cnet.addLink(node1=switch1, node2=switch2, port1=OFPP_CONTROLLER)
            info('* Connected Management Interface of Switch "%s" to Switch "%s"\n' % (str(switch1), str(switch2)))
    info('* Network Intialization Complete\n')
    
    mn=MininetFacade(net, cnet=cnet)
    CLI(mn) # POX should be receiving control traffic at this point, check by reading pox.log
    
    info('* Stopping POX\n')
    pox_process.send_signal(signal.SIGINT)
    pox_process.wait()
    info('* Stopping Data Network\n')
    net.stop()
    info('* Stopping Control Network\n')
    cnet.stop()

if __name__ == '__main__':
    setLogLevel( 'info' )
    run()
