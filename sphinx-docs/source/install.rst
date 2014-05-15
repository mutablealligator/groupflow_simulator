Installation and Usage
======================

Installing Dependencies
~~~~~~~~~~~~~~~~~~~~~~~
GroupFlow is implemented as a set of modules for `POX <http://www.noxrepo.org/pox/about-pox/>`_, a Python based SDN controller. POX is available at the `POX GitHub Repository <https://github.com/noxrepo/pox>`_, and further instructions on POX installation and usage can be found at the `POX Wiki <https://openflow.stanford.edu/display/ONL/POX+Wiki>`_.

GroupFlow is designed for use with real SDN networks, as well as Mininet emulated networks. Details on the installation and usage of Mininet can be found at `mininet.org <http://mininet.org/download/>`_.

The core POX modules do not have any further dependencies, but the supporting test scripts may rely on the `NumPy <http://www.numpy.org/>`_ and `SciPy <http://www.scipy.org/>`_ libraries.


Downloading GroupFlow
~~~~~~~~~~~~~~~~~~~~~

The latest version of GroupFlow can be downloaded directly from the GroupFlow github repository, as follows:

::
    
    git clone https://github.com/alexcraig/GroupFlow.git

To install into an existing POX installation, merge the "pox" directory in the GroupFlow repository with the base folder of your POX installation. This will place new files into the "pox/pox/lib/packet", "pox/pox/misc", and "pox/pox/openflow" directories.

The "groupflow_scripts" directory contains test scripts which are capable of sending and receiving source specific multicast traffic, as well as automating topology configuration and launching for Mininet network emulation.


Usage
~~~~~

To start POX with GroupFlow functionality, POX must be launched with the "openflow.discovery", "openflow.igmp_manager", "openflow.flow_tracker" and "openflow.groupflow" modules enabled. For example:

::
    
    pox.py openflow.discovery openflow.flow_tracker --query_interval=1 --link_max_bw=30 --link_cong_threshold=30 --avg_smooth_factor=0.65 --log_peak_usage=True openflow.igmp_manager openflow.groupflow --util_link_weight=10 --link_weight_type=linear log.level --WARNING --openflow.flow_tracker=INFO --openflow.igmp_manager=DEBUG

For more details on optional command line arguments, please see the API documentation for the relevant modules.

The modules can be tested with the provided Mininet automation script, which will generate a simple topology and manage the launch of POX with GroupFlow modules enabled (using default settings). Note that as this script invoke Mininet, it must be run as root. For example:

::
    
    ./sudo python mininet_multicast_pox.py