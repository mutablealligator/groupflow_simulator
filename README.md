GroupFlow
=========

This project aims to create a POX fork of the NOX-Classic Castflow implementation, supplemented with full IGMP v3 support implemented in the controller. This will allow the Castflow multicast proposals to be used in a live OpenFlow SDN network with standard IGMP v3 hosts. The end goal of this project is to provide a testbed for developers aiming to test new multicast ideas in a software defined network.

Development Progress
--------------------
Development thus far has focused on the OpenFlow IGMP v3 controller which is now functionally complete, with the following exceptions:

* The IGMPManager module implements the multicast router component of IGMP v3, but not the multicast host component. This should have no impact on the multicast functionality as long as all routers in the network are under the administrative domain of the same controller, but the lack of the host component on the router may cause issues if routers under a different administrative domain are connected to the network.
* IGMP v1 and v2 compatibility modes are not implemented. IGMPManager The module should only be expected to function correctly in a network composed entirely of IGMP v3 hosts.
* IGMP v3 is not emulated exactly on each router. Instead, all routers in the SDN controller domain are logically considered as a single IGMP router whose ports consist of the set of all ports which do not face adjacent routers. Because of this, IGMP querrier selection mechanisms are not implemented.

The multicast controller module is not yet implemented, and for testing purposes the IGMP v3 module simply installs flows to flood any multicast packets throughout the network. Because of this, the module should only be tested with loop free topologies at this point.

Installation
------------

To install into an existing POX installation, merge the "pox" directory located inside the "pox_version" directory in this repository with the base folder of your POX installation. This will place new files into the "pox/pox/lib/packet" and "pox/pox/openflow" directories.

The "pythontest" directory contains test scripts which are capable of sending and receiving source specific multicast traffic, as well as automating topology configuration and launching for Mininet network emulation.

Usage
-----

To start POX with GroupFlow functionality, POX must be launched with the "openflow.discovery" and "openflow.igmp_manager" modules enabled. For example:

    ./pox.py samples.pretty_log openflow.discovery openflow.igmp_manager forwarding.l3_learning log.level --WARNING --openflow.igmp_manager=INFO    

The modules can be tested with the provided Mininet automation script, which will generate a simple topology with multicast senders and receivers. Note that as this script invoke Mininet, it must be run as root. For example:

    ./sudo python mininet_multicast_pox.py

The IGMPManager module is designed to provide information to a multicast routing module by raising MulticastGroupEvents whenever the desired multicast reception state changes. These events consist of the following information:

* event.router_dpid: The DPID of the router for which updated reception state is being reported.
* event.adjacency_map: A map of the complete router adjacency learned by the IGMPManager. The map stores information in the form (Router with DPID router_dpid_1 is linked with router with DPID router_dpid_2 through port port_num):


    event.adjacency_map[router_dpid_1][router_dpid_2] = port_num  


* event.desired_reception: A map of the desired reception state for each multicast group / port. The map stores information in the form (an empty list indicates reception from all sources is desired):


    self.receiving_interfaces[multicast_address][port_index] = [list of desired sources addresses]
