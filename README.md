GroupFlow
=========

This project aims to create a POX fork of the NOX-Classic Castflow implementation, supplemented with full IGMP v3 support implemented in the controller. This will allow the Castflow multicast proposals to be used in a live OpenFlow SDN network with standard IGMP v3 hosts. The end goal of this project is to provide a testbed for developers aiming to test new multicast ideas in a software defined network.

![GroupFlow deployment](https://github.com/alexcraig/GroupFlow/blob/master/docs/CastflowDeploymentDiagram.png?raw=true)

Development Progress
--------------------
The OpenFlow IGMP v3 controller is now functionally complete, with the following exceptions:

* The IGMPManager module implements the multicast router component of IGMP v3, but not the multicast host component. This should have no impact on the multicast functionality as long as all routers in the network are under the administrative domain of the same controller, but the lack of the host component on the router may cause issues if routers under a different administrative domain are connected to the network.
* IGMP v1 and v2 compatibility modes are not implemented. IGMPManager The module should only be expected to function correctly in a network composed entirely of IGMP v3 hosts.
* IGMP v3 is not emulated exactly on each router. Instead, all routers in the SDN controller domain are logically considered as a single IGMP router whose ports consist of the set of all ports which do not face adjacent routers. Because of this, IGMP querrier selection mechanisms are not implemented.

The multicast controller module is now functionally complete.

Testing automation using BRITE topologies is not yet implemented.

Installation
------------

To install into an existing POX installation, merge the "pox" directory located inside the "pox_version" directory in this repository with the base folder of your POX installation. This will place new files into the "pox/pox/lib/packet" and "pox/pox/openflow" directories.

The "pythontest" directory contains test scripts which are capable of sending and receiving source specific multicast traffic, as well as automating topology configuration and launching for Mininet network emulation.

Usage
-----

To start POX with GroupFlow functionality, POX must be launched with the "openflow.discovery", "misc.groupflow_event_tracer", "openflow.igmp_manager", and "openflow.groupflow" modules enabled. For example:

    ./pox.py samples.pretty_log openflow.discovery misc.groupflow_event_tracer openflow.igmp_manager openflow.groupflow log.level --WARNING --openflow.igmp_manager=INFO --openflow.groupflow=INFO --misc.groupow_event_tracer=INFO   

The modules can be tested with the provided Mininet automation script, which will generate a simple topology with multicast senders and receivers. Note that as this script invoke Mininet, it must be run as root. For example:

    ./sudo python mininet_multicast_pox.py
    
IGMP Manager Module
-------------------

The IGMP Manager module is designed to provide information to a multicast routing module by raising the following events:

### MulticastGroupEvents

MulticastGroupEvents are raised whenever the multicast reception state of a router changes. These events consist of the following information:

* event.router_dpid: The DPID of the router for which updated reception state is being reported.

* event.desired_reception: A map of the desired reception state for each multicast group / port. The map stores information in the form (an empty list indicates reception from all sources is desired):


    self.receiving_interfaces[multicast_address][port_index] = [list of desired sources addresses]
    
### MulticastTopoEvent

MulticastTopoEvents are raised whenever a change in the network topology occurs which would be relevant to the multicast routing module. These events consist of the following information:

* event.adjacency_map: A map of the complete router adjacency learned by the IGMPManager. The map stores information in the form (Router with DPID router_dpid_1 is linked with router with DPID router_dpid_2 through port port_num):

    event.adjacency_map[router_dpid_1][router_dpid_2] = port_num
    

GroupFlow Module
----------------

The GroupFlow module implements multicast routing exactly as implemented in the original [CastFlow](https://github.com/caioviel/CastFlow) implementation. Multicast routing records are stored for each combination of multicast group and source address. For each of these records the GroupFlow module will calculate a minimum spanning tree using Prim's algorithm from the multicast source to all routers in the network (where each edge is weighted according to the number of hops from the multicast source). Branches of this tree which correspond to active multicast receivers are installed into the network through OpenFlow, and the spanning tree is only recalculated when the network topology changes. This should enable rapid changes of multicast group, as there is no need to completely recalculate the multicast tree when new receivers join a group.
