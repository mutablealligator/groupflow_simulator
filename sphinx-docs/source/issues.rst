Development Progress and Known Issues
=====================================

Current Development State
-------------------------

The OpenFlow IGMP v3 controller is now functionally complete, with the following exceptions:

* The IGMPManager module implements the multicast router component of IGMP v3, but not the multicast host component. This should have no impact on the multicast functionality as long as all routers in the network are under the administrative domain of the same controller, but the lack of the host component on the router may cause issues if routers under a different administrative domain are connected to the network.

* IGMP v1 and v2 compatibility modes are not implemented. IGMPManager The module should only be expected to function correctly in a network composed entirely of IGMP v3 hosts.

* IGMP v3 is not emulated exactly on each router. Instead, all routers in the SDN controller domain are logically considered as a single IGMP router whose ports consist of the set of all ports which do not face adjacent routers. Because of this, IGMP querrier selection mechanisms are not implemented.

The multicast controller module is now functionally complete, and optionally can make use of traffic estimation provided by the flow tracker module to load balance multicast traffic.

Known Issues
------------

Due to limitations in the OpenFlow v1.0 specification, the flow tracker module is not capable of determining the maximum bandwidth associated with a particular link. If load balancing operations is desired, all load balanced links must have the same maximum bandwidth, and this maximum bandwidth must be provided as a command line parameter.

