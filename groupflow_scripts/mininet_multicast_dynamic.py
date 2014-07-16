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
from multiprocessing import Process, Pipe
import numpy as np
import traceback

# Hardcoded purely for testing / debug, these will be moved once functionality is stable
NUM_GROUPS = 25
ARRIVAL_RATE = 5 * (1.0 / 60)
SERVICE_RATE = 1.0 / 60
TRIAL_DURATION_SECONDS = 60.0 * 5
RECEIVERS_AT_TRIAL_START = 5
STATS_RECORDING_INTERVAL = 5

def mcastTestDynamic(topo, hosts = [], log_file_name = 'test_log.log', util_link_weight = 10, link_weight_type = 'linear', replacement_mode='none', pipe = None):
    test_groups = []
    test_success = True

    # Launch the external controller
    pox_arguments = []
    static_link_weight = 0
    if util_link_weight == 0:
        static_link_weight = 1
        
    if 'periodic' in replacement_mode:
        pox_arguments = ['pox.py', 'log', '--file=pox.log,w', 'openflow.discovery', '--link_timeout=30', 'openflow.keepalive',
                'openflow.flow_tracker', '--query_interval=1', '--link_max_bw=19', '--link_cong_threshold=13', '--avg_smooth_factor=0.5', '--log_peak_usage=True',
                'misc.benchmark_terminator', 'openflow.igmp_manager', 'misc.groupflow_event_tracer',
                'openflow.groupflow', '--static_link_weight=' + str(static_link_weight), '--util_link_weight=' + str(util_link_weight), '--link_weight_type=' + link_weight_type, '--flow_replacement_mode=' + replacement_mode,
                '--flow_replacement_interval=10',
                'log.level', '--WARNING', '--openflow.flow_tracker=INFO']
    else:
        pox_arguments = ['pox.py', 'log', '--file=pox.log,w', 'openflow.discovery', '--link_timeout=30', 'openflow.keepalive',
                'openflow.flow_tracker', '--query_interval=1', '--link_max_bw=19', '--link_cong_threshold=13', '--avg_smooth_factor=0.5', '--log_peak_usage=True',
                'misc.benchmark_terminator', 'openflow.igmp_manager', 'misc.groupflow_event_tracer',
                'openflow.groupflow', '--static_link_weight=' + str(static_link_weight), '--util_link_weight=' + str(util_link_weight), '--link_weight_type=' + link_weight_type, '--flow_replacement_mode=' + replacement_mode,
                '--flow_replacement_interval=10',
                'log.level', '--WARNING', '--openflow.flow_tracker=INFO']
    print 'Launching external controller: ' + str(pox_arguments[0])
    print 'Launch arguments:'
    print ' '.join(pox_arguments)
    
    with open(os.devnull, "w") as fnull:
        pox_process = Popen(pox_arguments, stdout=fnull, stderr=fnull, shell=False, close_fds=True)
        # Allow time for the log file to be generated
        sleep(1)
    
    # Determine the flow tracker log file
    pox_log_file = open('./pox.log', 'r')
    flow_log_path = None
    event_log_path = None
    got_flow_log_path = False
    got_event_log_path = False
    while (not got_flow_log_path) or (not got_event_log_path):
        pox_log = pox_log_file.readline()

        if 'Writing flow tracker info to file:' in pox_log:
            pox_log_split = pox_log.split()
            flow_log_path = pox_log_split[-1]
            got_flow_log_path = True
        
        if 'Writing event trace info to file:' in pox_log:
            pox_log_split = pox_log.split()
            event_log_path = pox_log_split[-1]
            got_event_log_path = True
            
            
    print 'Got flow tracker log file: ' + str(flow_log_path)
    print 'Got event trace log file: ' + str(event_log_path)
    print 'Controller initialized'
    pox_log_offset = pox_log_file.tell()
    pox_log_file.close()
    # External controller launched
    
    # Launch Mininet
    net = Mininet(topo, controller=RemoteController, switch=OVSSwitch, link=TCLink, build=False, autoSetMacs=True)
    # pox = RemoteController('pox', '127.0.0.1', 6633)
    net.addController('pox', RemoteController, ip = '127.0.0.1', port = 6633)
    net.start()
    for switch_name in topo.get_switch_list():
        #print switch_name + ' route add -host 127.0.0.1 dev lo'
        net.get(switch_name).controlIntf = net.get(switch_name).intf('lo')
        net.get(switch_name).cmd('route add -host 127.0.0.1 dev lo')
        #print 'pox' + ' route add -host ' + net.get(switch_name).IP() + ' dev lo'
        net.get('pox').cmd('route add -host ' + net.get(switch_name).IP() + ' dev lo')
        #print net.get(switch_name).cmd('ifconfig')
        
    topo.mcastConfig(net)
    # Wait for controller topology discovery
    controller_init_sleep_time = 10
    print 'Waiting ' + str(controller_init_sleep_time) + ' seconds to allow for controller topology discovery.'
    sleep(controller_init_sleep_time)
    # Mininet launched
    
    # Generate the test groups, and launch the sender applications
    rand_seed = int(time())
    print 'Using random seed: ' + str(rand_seed)
    np.random.seed(rand_seed)
    
    trial_start_time = time() + (NUM_GROUPS * 2)    # Assume group configuration, and initial receiver init will take under 2 seconds per group
    trial_end_time = trial_start_time + TRIAL_DURATION_SECONDS
    mcast_group_last_octet = 1
    mcast_port = 5010
    for i in range(0, NUM_GROUPS):
        mcast_ip = '224.1.1.{last_octet}'.format(last_octet = str(mcast_group_last_octet))
        test_group = DynamicMulticastGroupDefinition(net.hosts, mcast_ip, mcast_port, mcast_port + 1)
        print 'Generating events for group: ' + mcast_ip
        test_group.generate_receiver_events(trial_start_time, TRIAL_DURATION_SECONDS, RECEIVERS_AT_TRIAL_START, ARRIVAL_RATE, SERVICE_RATE)
        test_group.launch_sender_application()
        test_groups.append(test_group)
        mcast_group_last_octet += 1
        mcast_port += 2
    # Test groups generated
    
    # Launch initial receiver applications
    for group in test_groups:
        group.update_receiver_applications(trial_start_time)
        sleep(uniform(0, 2))
    
    # Wait for trial run start time
    sleep_time = trial_start_time - time()
    if sleep_time < 0:
        print 'WARNING: sleep_time is negative!'
    else:
        print 'Waiting ' + str(sleep_time) + ' seconds to allow for group initialization.'
        sleep(sleep_time)   # Allow time for the controller to detect the topology
    # Trial has started at this point
    
    try:
        while True:
            cur_time = time()
            if cur_time > trial_end_time:
                print 'Reached trial end at time: ' + str(cur_time)
                break
                
            next_event_time = trial_end_time
            for group in test_groups:
                group.update_receiver_applications(cur_time)
                next_event = group.get_next_receiver_event()
                if next_event is not None and next_event[0] < next_event_time:
                    next_event_time = next_event[0]
            
            sleep_time = next_event_time - time()
            if sleep_time < 0:
                print 'WARNING: sleep_time (' + str(sleep_time) + ') is negative!'
            else:
                #print 'Waiting ' + str(sleep_time) + ' for next event.\n'
                sleep(sleep_time)
        
        print 'Terminating network applications'
        for group in test_groups:
            group.terminate_group()
        print 'Network applications terminated'
        print 'Terminating controller'
        pox_process.send_signal(signal.SIGINT)
        sleep(1)
        print 'Waiting for controller termination...'
        pox_process.send_signal(signal.SIGKILL)
        pox_process.wait()
        print 'Controller terminated'
        pox_process = None
        net.stop()
        sleep(3)
        
        # Print packet loss statistics
        recv_packets = sum(group.get_total_recv_packets() for group in test_groups)
        lost_packets = sum(group.get_total_lost_packets() for group in test_groups)
        packet_loss = 0
        if (recv_packets + lost_packets) != 0:
            packet_loss = (float(lost_packets) / (float(recv_packets) + float(lost_packets))) * 100
        print 'RecvPackets: ' + str(recv_packets) + '  LostPackets: ' + str(lost_packets) + '  PacketLoss: ' + str(packet_loss) + '%'
        
        # Calculate mean service time (sanity check to see that exponential service time generation is working as intended)
        num_apps = 0
        total_service_time = 0
        for group in test_groups:
            for recv_app in group.receiver_applications:
                num_apps += 1
                total_service_time += recv_app.service_time
        print 'Average Service Time: ' + str(total_service_time / num_apps)
        
        # Delete log file if test encountered an error, or write the statistic log file if the run was succesfull
        if not test_success:
            call('rm -rf ' + str(flow_log_path), shell=True)
            call('rm -rf ' + str(event_log_path), shell=True)
        else:
            write_dynamic_stats_log(log_file_name, flow_log_path, event_log_path, test_groups, topo, ARRIVAL_RATE, SERVICE_RATE, 
                    RECEIVERS_AT_TRIAL_START, trial_start_time, trial_end_time, STATS_RECORDING_INTERVAL)
        
    except BaseException as e:
        traceback.print_exc()
        test_success = False
    
    if pipe is not None:
        pipe.send(test_success)
        pipe.close()

topos = { 'mcast_test': ( lambda: MulticastTestTopo() ) }

def print_usage_text():
    print 'GroupFlow Multicast Testing with Mininet'
    print 'Usage - Automated Benchmarking:'
    print '> mininet_multicast_pox <topology_path> <iterations_to_run> <log_file_prefix> <index_of_first_log_file> <parameter_sets (number is variable and unlimited)>'
    print 'Parameter sets have the form: flow_replacement_mode,link_weight_type,util_link_weight'
    print 'The topology path "manhattan" is currently hardcoded to generate a 20 Mbps, 5x5 Manhattan grid topology'

if __name__ == '__main__':
    setLogLevel( 'info' )
    
    # Uncomment for easy debug testing
    # topo = ManhattanGridTopo(5, 4, 20, 1, False)
    # hosts = topo.get_host_list()
    # mcastTestDynamic(topo, hosts, 'test.log', 10, 'linear', 'none')
    # sys.exit()
    
    if len(sys.argv) >= 2:
        if '-h' in str(sys.argv[1]) or 'help' in str(sys.argv[1]):
            print_usage_text()
            sys.exit()
    
    if len(sys.argv) >= 6:
        # Automated simulations - Differing link usage weights in Groupflow Module
        log_prefix = sys.argv[3]
        num_iterations = int(sys.argv[2])
        first_index = int(sys.argv[4])
        util_params = []
        for param_index in range(5, len(sys.argv)):
            param_split = sys.argv[param_index].split(',')
            util_params.append((param_split[0], param_split[1], float(param_split[2])))
        topo = None
        if 'manhattan' in sys.argv[1]:
            print 'Generating Manhattan Grid Topology'
            topo = ManhattanGridTopo(5, 4, 20, 1, False)
        else:
            print 'Generating BRITE Specified Topology'
            topo = BriteTopo(sys.argv[1])
        hosts = topo.get_host_list()
        start_time = time()
        num_success = 0
        num_failure = 0
        print 'Simulations started at: ' + str(datetime.now())
        for i in range(0,num_iterations):
            for util_param in util_params:
                test_success = False
                while not test_success:
                    parent_pipe, child_pipe = Pipe()
                    p = Process(target=mcastTestDynamic, args=(topo, hosts, log_prefix + '_' + ','.join([util_param[0], util_param[1], str(util_param[2])]) + '_' + str(i + first_index) + '.log', util_param[2], util_param[1], util_param[0], child_pipe))
                    sim_start_time = time()
                    p.start()
                    p.join()
                    sim_end_time = time()
                    
                    # Make extra sure the network terminated cleanly
                    call(['python', 'kill_running_test.py'])
                    
                    test_success = parent_pipe.recv()
                    parent_pipe.close()
                    print 'Test Success: ' + str(test_success)
                    if test_success:
                        num_success += 1
                    else:
                        num_failure += 1
                print 'Simulation ' + str(i+1) + '_' + ','.join([util_param[0], util_param[1], str(util_param[2])]) + ' completed at: ' + str(datetime.now()) + ' (runtime: ' + str(sim_end_time - sim_start_time) + ' seconds)'
                
        end_time = time()
        print ' '
        print 'Simulations completed at: ' + str(datetime.now())
        print 'Total runtime: ' + str(end_time - start_time) + ' seconds'
        print 'Average runtime per sim: ' + str((end_time - start_time) / (num_iterations * len(util_params))) + ' seconds'
        print 'Number of failed sims: ' + str(num_failure)
        print 'Number of successful sims: ' + str(num_success)
