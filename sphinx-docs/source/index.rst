.. GroupFlow documentation master file, created by
   sphinx-quickstart on Tue May 13 09:22:00 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

*********************************************************
GroupFlow: Multicast Routing in Software Defined Networks
*********************************************************
   
:Author: `Alexander Craig <https://github.com/alexcraig>`_
:License: This software is licensed under the
  `Apache License, Version 2.0 <http://www.apache.org/licenses/LICENSE-2.0.html>`_.
  
Overview
========

This project aims to create a POX fork/extension of the NOX-Classic Castflow implementation, supplemented with full IGMP v3 support implemented in the controller. This will allow SDN multicast proposals to be deployed in a live OpenFlow SDN network with standard IGMPv3 hosts. The end goal of this project is to provide a testbed for developers aiming to test new multicast ideas in a software defined network.

.. image:: GroupflowDeploymentDiagram.png

Contents:

.. toctree::
   :maxdepth: 2
   
   install.rst
   igmp_manager.rst
   groupflow.rst
   flowtracker.rst
   benchmarking.rst
   

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

