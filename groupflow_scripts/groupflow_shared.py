from mininet.topo import *
from scipy.stats import truncnorm, tstd, poisson, expon
from numpy.random import randint, uniform
from datetime import datetime
import os
import signal

LATENCY_METRIC_MIN_AVERAGE_DELAY = 1
LATENCY_METRIC_MIN_MAXIMUM_DELAY = 2

class ReceiverLogStats(object):
    def __init__(self, filename, recv_bytes, recv_packets, lost_packets):
        self.filename = filename
        self.recv_bytes = int(recv_bytes)
        self.recv_packets = int(recv_packets)
        self.lost_packets = int(lost_packets)
        if lost_packets + recv_packets == 0:
            self.packet_loss = 0
        else:
            self.packet_loss = (float(lost_packets) / (float(lost_packets) + float(recv_packets))) * 100

    def debug_print(self):
        print 'Multicast Receiver Log: ' + str(self.filename)
        print 'RecvBytes: ' + str(self.recv_bytes) + ' RecvPackets: ' + str(self.recv_packets) + ' LostPackets: ' + str(self.lost_packets)
        print 'PacketLoss: ' + str(self.packet_loss) + '%'

        
class MulticastReceiverApplication(object):
    APP_STATE_PRELAUNCH = 1
    APP_STATE_RUNNING = 2
    APP_STATE_COMPLETE = 3
    
    def __init__(self, host, group_ip, mcast_port, echo_port, init_time, service_time):
        self.host = host
        self.group_ip = group_ip
        self.mcast_port = mcast_port
        self.echo_port = echo_port
        self.init_time = init_time
        self.service_time = service_time
        self.terminate_time = init_time + service_time
        self.log_filename = 'mcastlog_' + str(self.group_ip.replace('.', '_')) + '_' + str(host) + '_' + str(init_time) + '.log'
        self.app_process = None
        self.log_stats = None
        self.app_state = MulticastReceiverApplication.APP_STATE_PRELAUNCH
    
    def launch_receiver_application(self):
        if self.app_state == MulticastReceiverApplication.APP_STATE_PRELAUNCH and self.app_process is None:
            with open(os.devnull, "w") as fnull:
                vlc_rcv_command = ['python', './multicast_receiver_VLC.py', self.group_ip, str(self.mcast_port), str(self.echo_port), str(self.log_filename)]
                # print 'Running: ' + ' '.join(vlc_rcv_command)
                self.app_process = self.host.popen(vlc_rcv_command, stdout=fnull, stderr=fnull, close_fds=True, shell=False)
            
            self.app_state = MulticastReceiverApplication.APP_STATE_RUNNING
    
    def terminate_receiver_application(self):
        if self.app_state == MulticastReceiverApplication.APP_STATE_RUNNING and self.app_process is not None:
            # Terminate the application
            self.app_process.send_signal(signal.SIGINT)
            self.app_state = MulticastReceiverApplication.APP_STATE_COMPLETE
    
    def read_log_stats(self):
        if self.app_process is not None:
            self.app_process.wait()
            self.app_process = None
            
        # Read the application's log file and record relevant stats
        log_file = open(self.log_filename, 'r')
        for line in log_file:
            if 'RecvPackets:' in line:
                line_split = line.split(' ')
                recv_packets = line_split[0][len('RecvPackets:'):]
                recv_bytes = line_split[1][len('RecvBytes:'):]
                lost_packets = line_split[2][len('LostPackets:'):]
                self.log_stats = ReceiverLogStats(str(self.log_filename), recv_bytes, recv_packets, lost_packets)
                break
        log_file.close()
        
        # Remove the application log file
        os.remove(self.log_filename)
    
    def get_recv_packets(self):
        if self.log_stats is None:
            return 0
        else:
            return self.log_stats.recv_packets
    
    def get_lost_packets(self):
        if self.log_stats is None:
            return 0
        else:
            return self.log_stats.recv_packets
    
    def get_app_state(self):
        return self.app_state
    
    def __str__(self):
        return 'Recv-' + str(self.host)

        
class DynamicMulticastGroupDefinition(object):
    EVENT_RECEIVER_INIT = 'Recv_Init'
    EVENT_RECEIVER_TERMINATION = 'Recv_Term'
    
    def __init__(self, net_hosts, group_ip, mcast_port, echo_port):
        self.net_hosts = net_hosts
        self.group_ip = group_ip
        self.mcast_port = mcast_port
        self.echo_port = echo_port
        self.src_process = None
        self.receiver_applications = []
        self.event_list = None
    
    def generate_receiver_events(self, trial_start_time, trial_duration_seconds, arrival_rate, service_rate):
        """Generates receiver init and termination events.
        
        Receiver initialization events are generated as a poission process with arrival rate: arrival_rate (in receivers per second).
        Each receiver has an exponential service time with rate: service_rate.
        This should be called at the start of a simulation run, just after initialization of mininet.
        """
        if self.event_list is not None:
            return
        self.event_list = []
        
        # Find the number of arrivals in the interval [0, trial_duration_seconds]
        # Size = trial_duration_seconds, since we want the number of arrivals in trial_duration_seconds time units
        num_arrivals = sum(poisson.rvs(arrival_rate, size=trial_duration_seconds))
        # Once the number of arrivals is known, generate arrival times uniform on [0, trial_duration_seconds]
        arrival_times = []
        for i in range(0, num_arrivals):
            arrival_times.append(uniform(0, trial_duration_seconds))
        
        # Now, for each arrival, generate a corresponding receiver application and events
        for arrival_time in arrival_times:
            # Generate a service time
            service_time = expon(loc = 0, scale=(1.0 / service_rate)).rvs(1)[0]
            # Select a host through a uniform random distribution
            receiver = self.net_hosts[randint(0,len(self.net_hosts))]
            receiver = MulticastReceiverApplication(receiver, self.group_ip, self.mcast_port, self.echo_port, trial_start_time + arrival_time, service_time)
            self.receiver_applications.append(receiver)
            self.event_list.append((trial_start_time + arrival_time, DynamicMulticastGroupDefinition.EVENT_RECEIVER_INIT, receiver))
            self.event_list.append((trial_start_time + arrival_time + service_time, DynamicMulticastGroupDefinition.EVENT_RECEIVER_TERMINATION, receiver))
        
        # Sort the event list by time
        self.event_list = sorted(self.event_list, key=lambda tup: tup[0])
        
        # Debug printing
        for event in self.event_list:
            print 'Time:' + str(event[0]) + ' ' + str(event[1]) + ' ' + str(event[2])
    
    def launch_sender_application(self):
        """Launches the group sender application.
        
        This should be called at the start of a simulation run, after mininet is initialized.
        """
        if self.src_process is None:
            with open(os.devnull, "w") as fnull:
                vlc_command = ['vlc-wrapper', 'test_media.mp4', '-I', 'dummy', '--sout', '"#rtp{access=udp, mux=ts, proto=udp, dst=' + self.group_ip + ', port=' + str(self.mcast_port) + '}"', '--sout-keep', '--loop']
                sender = self.net_hosts[randint(0,len(self.net_hosts))]
                self.src_process = sender.popen(' '.join(vlc_command), stdout=fnull, stderr=fnull, close_fds=True, shell=True)
        
    def update_receiver_applications(self, current_time):
        """Launches/terminates receiver applications as specified by the current time and the event_list attribute."""
        while len(self.event_list) > 0 and self.event_list[0][0] <= current_time:
            event = self.event_list.pop(0)
            if event[1] == DynamicMulticastGroupDefinition.EVENT_RECEIVER_INIT:
                print 'Launching receiver ' + str(event[2]) + ' at time: ' + str(event[0])
                event[2].launch_receiver_application()
            elif event[1] == DynamicMulticastGroupDefinition.EVENT_RECEIVER_TERMINATION:
                event[2].terminate_receiver_application()
                print 'Terminating receiver ' + str(event[2]) + ' at time: ' + str(event[0])
    
    def terminate_group(self):
        """Terminates the sender application, as well as any receiver applications which are currently running.
        
        This should be called at the end of a simulation run, before terminating mininet.
        """
        if self.src_process is not None:
            # print 'Killing process with PID: ' + str(self.src_process.pid)
            self.src_process.terminate()
            self.src_process.kill()
        
        # TODO: Kill any receivers still running
        for recv_app in self.receiver_applications:
            if recv_app.app_state == MulticastReceiverApplication.APP_STATE_RUNNING:
                recv_app.terminate_receiver_application()
    
    def get_next_receiver_event(self):
        """Returns the receiver event at the head of the event list (or None if the list is empty)."""
        if len(self.event_list) > 0:
            return self.event_list[0]
        else:
            return None
    

class StaticMulticastGroupDefinition(object):
    """Class used to manage the launch and termination of a single group of multicast applications with static membership.
    
    Multicast groups managed by this class have the following properties:
    
    * Each group has a single sender, which is an instance of VLC streaming a file named "test_media.mp4" over UDP
    * The group may have an arbitrary number of receivers, which are all instances of multicast_receiver_VLC.py
    * The group sender and all receivers are all initialized at the same time, and are all terminated at the same time
    
    """
    def __init__(self, src_host, dst_hosts, group_ip, mcast_port, echo_port):
        self.src_host = src_host
        self.dst_hosts = dst_hosts
        self.group_ip = group_ip
        self.mcast_port = mcast_port
        self.echo_port = echo_port
        
        self.receiver_log_files = []    # Stores filenames
        self.receiver_log_stats = []    # Stores ReceiverLogStats objects
        
        
        self.src_process = None
        self.dst_processes = []
    
    def launch_mcast_applications(self, net):
        # print 'Initializing multicast group ' + str(self.group_ip) + ':' + str(self.mcast_port) + ' Echo port: ' + str(self.echo_port)
        with open(os.devnull, "w") as fnull:
            # self.src_process = net.get(self.src_host).popen(['python', './multicast_sender.py', self.group_ip, str(self.mcast_port), str(self.echo_port)], stdout=fnull, stderr=fnull, close_fds=True)
            vlc_command = ['vlc-wrapper', 'test_media.mp4', '-I', 'dummy', '--sout', '"#rtp{access=udp, mux=ts, proto=udp, dst=' + self.group_ip + ', port=' + str(self.mcast_port) + '}"', '--sout-keep', '--loop']
            # print 'Running: ' + ' '.join(vlc_command)
            self.src_process = net.get(self.src_host).popen(' '.join(vlc_command), stdout=fnull, stderr=fnull, close_fds=True, shell=True)
            
        for dst in self.dst_hosts:
            recv_log_filename = 'mcastlog_' + str(self.group_ip.replace('.', '_')) + '_' + str(dst) + '.log'
            with open(os.devnull, "w") as fnull:
                # self.dst_processes.append(net.get(dst).popen(['python', './multicast_receiver.py', self.group_ip, str(self.mcast_port), str(self.echo_port)], stdout=fnull, stderr=fnull, close_fds=True))
                vlc_rcv_command = ['python', './multicast_receiver_VLC.py', self.group_ip, str(self.mcast_port), str(self.echo_port), str(recv_log_filename)]
                # print 'Running: ' + ' '.join(vlc_rcv_command)
                self.dst_processes.append(net.get(dst).popen(vlc_rcv_command, stdout=fnull, stderr=fnull, close_fds=True, shell=False))
                self.receiver_log_files.append(recv_log_filename)
        
        print('Initialized multicast group ' + str(self.group_ip) + ':' + str(self.mcast_port)
                + ' Echo port: ' + str(self.echo_port) + ' # Receivers: ' + str(len(self.dst_processes)))
    
    def terminate_mcast_applications(self):
        if self.src_process is not None:
            # print 'Killing process with PID: ' + str(self.src_process.pid)
            # os.killpg(self.src_process.pid, signal.SIGTERM)
            self.src_process.terminate()
            self.src_process.kill()
            
        for proc in self.dst_processes:
            # print 'Killing process with PID: ' + str(proc.pid)
            proc.send_signal(signal.SIGINT)
            # proc.terminate()
            # proc.kill()
        
        print 'Signaled termination of multicast group ' + str(self.group_ip) + ':' + str(self.mcast_port) + ' Echo port: ' + str(self.echo_port)

    def wait_for_application_termination(self):
        if self.src_process is not None:
            self.src_process.wait()
            self.src_process = None
        
        for proc in self.dst_processes:
            proc.wait()
            
        for filename in self.receiver_log_files:
            log_file = open(filename, 'r')
            for line in log_file:
                if 'RecvPackets:' in line:
                    line_split = line.split(' ')
                    recv_packets = line_split[0][len('RecvPackets:'):]
                    recv_bytes = line_split[1][len('RecvBytes:'):]
                    lost_packets = line_split[2][len('LostPackets:'):]
                    log_stats = ReceiverLogStats(str(filename), recv_bytes, recv_packets, lost_packets)
                    #log_stats.debug_print()
                    self.receiver_log_stats.append(log_stats)
                    break
            log_file.close()
            # print 'Read ' + filename
            os.remove(filename)
            
        self.dst_processes = []
    
    def get_total_recv_packets(self):
        return sum(log.recv_packets for log in self.receiver_log_stats)

    def get_total_lost_packets(self):
        return sum(log.lost_packets for log in self.receiver_log_stats)
        
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
        rvs_sum = sum(rvs)
        rvs = [p / (rvs_sum/float(avg_group_size)) for p in rvs]
        
    prob_tuples = []
    for index, host in enumerate(hosts):
        prob_tuples.append((host, rvs[index]))
    
    return prob_tuples


def write_final_stats_log(final_log_path, flow_stats_file_path, event_log_file_path, membership_mean, membership_std_dev, membership_avg_bound, test_groups, group_launch_times, topography):
    def write_current_stats(log_file, link_bandwidth_usage_Mbps, switch_num_flows, switch_average_load, response_times, cur_group_index, group):
        link_bandwidth_list = []
        total_num_flows = 0
        
        for switch_dpid in link_bandwidth_usage_Mbps:
            for port_no in link_bandwidth_usage_Mbps[switch_dpid]:
                link_bandwidth_list.append(link_bandwidth_usage_Mbps[switch_dpid][port_no])
        
        for switch_dpid in switch_num_flows:
            total_num_flows += switch_num_flows[switch_dpid]
        
        net_wide_avg_load = 0
        for switch_dpid in switch_average_load:
            net_wide_avg_load += switch_average_load[switch_dpid]
        net_wide_avg_load = float(net_wide_avg_load) / len(switch_average_load)
        
        avg_response_time = sum(response_times) / float(len(response_times))
        avg_network_time = sum(network_times) / float(len(network_times))
        avg_processing_time = sum(processing_times) / float(len(processing_times))
        
        average_link_bandwidth_usage = sum(link_bandwidth_list) / float(len(link_bandwidth_list))
        traffic_concentration = 0
        if average_link_bandwidth_usage != 0:
            traffic_concentration = max(link_bandwidth_list) / average_link_bandwidth_usage
        link_util_std_dev = tstd(link_bandwidth_list)
        
        log_file.write('Group:' + str(cur_group_index))
        log_file.write(' NumReceivers:' + str(len(group.dst_hosts)))
        log_file.write(' TotalNumFlows:' + str(total_num_flows))
        log_file.write(' MaxLinkUsageMbps:' + str(max(link_bandwidth_list)))
        log_file.write(' AvgLinkUsageMbps:' + str(average_link_bandwidth_usage))
        log_file.write(' TrafficConcentration:' + str(traffic_concentration))
        log_file.write(' LinkUsageStdDev:' + str(link_util_std_dev))
        log_file.write(' ResponseTime:' + str(avg_response_time))
        log_file.write(' NetworkTime:' + str(avg_network_time))
        log_file.write(' ProcessingTime:' + str(avg_processing_time))
        log_file.write(' SwitchAvgLoadMbps:' + str(net_wide_avg_load))
        log_file.write('\n')
 
    switch_num_flows = {}   # Dictionary of number of currently installed flows, keyed by switch_dpid
    switch_average_load = {}    # Dictionary of switch average load, keyed by switch_dpid
    link_bandwidth_usage_Mbps = {} # Dictionary of dictionaries: link_bandwidth_usage_Mbps[switch_dpid][port_no]
    cur_group_index = 0
    cur_time = 0
    cur_switch_dpid = None
    
    final_log_file = open(final_log_path, 'w')
    # Write out scenario params
    num_receivers_list = []
    for group in test_groups:
        num_receivers_list.append(len(group.dst_hosts))
    avg_num_receivers = sum(num_receivers_list) / float(len(num_receivers_list))
    
    # Calculate packet loss
    recv_packets = 0
    lost_packets = 0
    for group in test_groups:
        recv_packets += group.get_total_recv_packets()
        lost_packets += group.get_total_lost_packets()
    packet_loss = 0
    if recv_packets + lost_packets != 0:
        packet_loss = (float(lost_packets) / (recv_packets + lost_packets)) * 100
    
    final_log_file.write('GroupFlow Performance Simulation: ' + str(datetime.now()) + '\n')
    final_log_file.write('FlowStatsLogFile:' + str(flow_stats_file_path) + '\n')
    final_log_file.write('EventTraceLogFile:' + str(event_log_file_path) + '\n')
    final_log_file.write('Membership Mean:' + str(membership_mean) + ' StdDev:' + str(membership_std_dev) + ' AvgBound:' + str(membership_avg_bound) + ' NumGroups:' + str(len(test_groups) - 1) + ' AvgNumReceivers:' + str(avg_num_receivers) + '\n')
    final_log_file.write('Topology:' + str(topography) + ' NumSwitches:' + str(len(topography.switches())) + ' NumLinks:' + str(len(topography.links())) + ' NumHosts:' + str(len(topography.hosts())) + '\n')
    final_log_file.write('RecvPackets:' + str(recv_packets) + ' LostPackets:' + str(lost_packets) + ' AvgPacketLoss:' + str(packet_loss) + '\n\n')
    
    flow_log_file = open(flow_stats_file_path, 'r')
    response_times = []
    network_times = []
    processing_times = []
    
    for line in flow_log_file:
        # This line specifies that start of stats for a new switch and time instant
        if 'PortStats' in line:
            line_split = line.split()
            switch_dpid = line_split[1][len('Switch:'):]
            num_flows = int(line_split[2][len('NumFlows:'):])
            cur_time = float(line_split[4][len('IntervalEndTime:'):])
            response_time = float(line_split[5][len('ResponseTime:'):])
            network_time = float(line_split[6][len('NetworkTime:'):])
            processing_time = float(line_split[7][len('ProcessingTime:'):])
            avg_load = float(line_split[8][len('AvgSwitchLoad:'):])

            cur_switch_dpid = switch_dpid
            
            # print 'Got stats for switch: ' + str(switch_dpid)
            # print 'Cur Time: ' + str(cur_time) + '    Next Group Launch: ' + str(group_launch_times[cur_group_index])
            
            # First, check to see if a new group has been initialized before this time, and log the current flow stats if so
            if cur_group_index < len(group_launch_times) and cur_time > group_launch_times[cur_group_index]:
                cur_group_index += 1
                if(cur_group_index > 1):
                    write_current_stats(final_log_file, link_bandwidth_usage_Mbps, switch_num_flows, switch_average_load, response_times, cur_group_index - 2, test_groups[cur_group_index - 2])
                    response_times = []
                    network_times = []
                    processing_times = []
            
            response_times.append(response_time)
            network_times.append(network_time)
            processing_times.append(processing_time)
            switch_num_flows[cur_switch_dpid] = num_flows
            switch_average_load[cur_switch_dpid] = avg_load
            
        # This line specifies port specific stats for the last referenced switch
        if 'PSPort' in line:
            line_split = line.split()
            port_no = int(line_split[0][len('PSPort:'):])
            bandwidth_usage = float(line_split[3][len('AvgBandwidth:'):])
            if(port_no == 65533):
                # Ignore connections to the controller for these calculations
                continue
                
            if cur_switch_dpid not in link_bandwidth_usage_Mbps:
                link_bandwidth_usage_Mbps[cur_switch_dpid] = {}
            link_bandwidth_usage_Mbps[cur_switch_dpid][port_no] = bandwidth_usage
    
    # Print the stats for the final multicast group
    write_current_stats(final_log_file, link_bandwidth_usage_Mbps, switch_num_flows, switch_average_load, response_times, cur_group_index - 1, test_groups[cur_group_index - 1])
    
    flow_log_file.close()
    final_log_file.close()


class BriteTopo(Topo):
    def __init__(self, brite_filepath):
        # Initialize topology
        Topo.__init__( self )
        
        self.hostnames = []
        self.switch_names = []
        self.routers = []
        self.edges = []
        self.file_path = brite_filepath
        
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
            switch = self.addSwitch('s' + str(node_id), inband = False)
            host = self.addHost('h' + str(node_id), ip = '10.0.0.' + str(node_id + 1))
            self.addLink(switch, host, bw=1000, use_htb=True)	# TODO: Better define link parameters for hosts
            self.routers.append(switch)
            self.switch_names.append('s' + str(node_id))
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
            self.edges.append(('s' + str(switch_id_1), 's' + str(switch_id_2), float(line_split[4])))
            self.edges.append(('s' + str(switch_id_2), 's' + str(switch_id_1), float(line_split[4])))
            bandwidth_Mbps = float(line_split[5])
            print 'Adding link between switch ' + str(switch_id_1) + ' and ' + str(switch_id_2) + '\n\tRate: ' \
                + str(bandwidth_Mbps) + ' Mbps\tDelay: ' + delay_ms
            # params = {'bw':bandwidth_Mbps, 'delay':delay_ms}]
            # TODO: Figure out why setting the delay won't work
            self.addLink(self.routers[switch_id_1], self.routers[switch_id_2], bw=bandwidth_Mbps, delay=delay_ms, max_queue_size=1000, use_htb=True)
        
        file.close()
    
    def get_controller_placement(self, latency_metric = LATENCY_METRIC_MIN_AVERAGE_DELAY):
        delay_metric_value = sys.float_info.max
        source_node_id = None

        for src_switch in self.routers:
            # Compute the shortest path tree for each possible controller placement
            nodes = set(self.routers)
            graph = defaultdict(list)
            for src,dst,cost in self.edges:
                graph[src].append((cost, dst))
         
            path_tree_map = defaultdict(lambda : None)
            queue, seen = [(0,src_switch,())], set()
            while queue:
                (cost,node1,path) = heappop(queue)
                if node1 not in seen:
                    seen.add(node1)
                    path = (cost, node1, path)
                    path_tree_map[node1] = path
         
                    for next_cost, node2 in graph.get(node1, ()):
                        if node2 not in seen:
                            heappush(queue, (cost + next_cost, node2, path))

            # Calculate the metric value for this position
            if latency_metric == LATENCY_METRIC_MIN_AVERAGE_DELAY:
                sum_delay = 0
                for receiver in path_tree_map:
                    sum_delay += path_tree_map[receiver][0]
                avg_delay = sum_delay / float(len(path_tree_map))
                if avg_delay < delay_metric_value:
                    source_node_id = src_switch
                    delay_metric_value = avg_delay
                    
            elif latency_metric == LATENCY_METRIC_MIN_MAXIMUM_DELAY:
                max_delay = 0
                for receiver in path_tree_map:
                    if path_tree_map[receiver][0] > max_delay:
                        max_delay = path_tree_map[receiver][0]
                if max_delay < delay_metric_value:
                    source_node_id = src_switch
                    delay_metric_value = max_delay
        
        print 'Found best controller placement at ' + str(source_node_id) + ' with metric: ' + str(delay_metric_value)
        return source_node_id, delay_metric_value
    
    def get_host_list(self):
        return self.hostnames
    
    def get_switch_list(self):
        return self.switch_names
    
    def mcastConfig(self, net):
        for hostname in self.hostnames:
            net.get(hostname).cmd('route add -net 224.0.0.0/4 ' + hostname + '-eth0')
    
    def __str__(self):
        return self.file_path


class ManhattanGridTopo(Topo):
    def __init__(self, grid_x, grid_y, link_Mbps, link_delay_ms, edge_interconnect = False):
        # Initialize topology
        Topo.__init__( self )
        
        self.hostnames = []
        self.switch_names = []
        
        self.routers = []
        self.grid_routers = {}  # Stores the same objects as self.routers, but keyed as a 2 dimensional map (self.grid_routers[x_coord][y_coord])
        self.edges = []
        
        print 'Generating Manhattan Grid Topology with Parameters:'
        print 'Grid X: ' + str(grid_x) + ' Grid Y: ' + str(grid_y) + ' TotalNumSwitches: ' + str(grid_x * grid_y)
        print 'Link Bandwidth: ' + str(link_Mbps) + ' Mbps  \tLink Delay: ' + str(link_delay_ms) + ' ms'
        print 'Edge Interconnect: ' + str(edge_interconnect)
        
        # Generate an X * Y grid of routers
        host_id = 1
        for x in range(0, grid_x):
            for y in range(0, grid_y):
                switch = self.addSwitch('s' + str(x) + str(y), inband = False)
                host = self.addHost('h' + str(host_id), ip = '10.0.0.' + str(host_id))
                self.addLink(switch, host, bw=1000, use_htb=True)	# TODO: Better define link parameters for hosts
                
                self.routers.append(switch)
                if x not in self.grid_routers:
                    self.grid_routers[x] = {}
                self.grid_routers[x][y] = switch
                self.switch_names.append('s' + str(x) + str(y))
                self.hostnames.append('h' + str(host_id))
                host_id += 1
        
        # Add links between all adjacent nodes (not including diagonal adjacencies)
        for x in range(0, grid_x - 1):
            for y in range(0, grid_y):
                # Add the X direction link
                self.edges.append(('s' + str(x) + str(y), 's' + str(x + 1) + str(y), link_delay_ms))
                self.edges.append(('s' + str(x + 1) + str(y), 's' + str(x) + str(y), link_delay_ms))
                print 'Adding link between switch ' + 's' + str(x) + str(y) + ' and ' + 's' + str(x + 1) + str(y) + '\n\tRate: ' \
                        + str(link_Mbps) + ' Mbps\tDelay: ' + str(link_delay_ms)
                self.addLink(self.grid_routers[x][y], self.grid_routers[x + 1][y], bw=link_Mbps, delay=str(link_delay_ms) + 'ms', max_queue_size=1000, use_htb=True)
        
        for y in range(0, grid_y - 1):
            for x in range(0, grid_x):
                # Add the Y direction link
                self.edges.append(('s' + str(x) + str(y), 's' + str(x) + str(y + 1), link_delay_ms))
                self.edges.append(('s' + str(x) + str(y + 1), 's' + str(x) + str(y), link_delay_ms))
                print 'Adding link between switch ' + 's' + str(x) + str(y) + ' and ' + 's' + str(x) + str(y + 1) + '\n\tRate: ' \
                        + str(link_Mbps) + ' Mbps\tDelay: ' + str(link_delay_ms)
                self.addLink(self.grid_routers[x][y], self.grid_routers[x][y + 1], bw=link_Mbps, delay=str(link_delay_ms) + 'ms', max_queue_size=1000, use_htb=True)
        
        # Interconnect the grid edges if the edge_interconnect flag is set
        if edge_interconnect:
            for x in range(0, grid_x):
                self.edges.append(('s' + str(x) + str(grid_y - 1), 's' + str(x) + str(0), link_delay_ms))
                self.edges.append(('s' + str(x) + str(0), 's' + str(x) + str(grid_y - 1), link_delay_ms))
                print 'Adding link between switch ' + 's' + str(x) + str(0) + ' and ' + 's' + str(x) + str(grid_y - 1) + '\n\tRate: ' \
                        + str(link_Mbps) + ' Mbps\tDelay: ' + str(link_delay_ms)
                self.addLink(self.grid_routers[x][0], self.grid_routers[x][grid_y - 1], bw=link_Mbps, delay=str(link_delay_ms) + 'ms', max_queue_size=1000, use_htb=True)
                
            for y in range(0, grid_y):
                self.edges.append(('s' + str(0) + str(y), 's' + str(grid_x - 1) + str(y), link_delay_ms))
                self.edges.append(('s' + str(grid_x - 1) + str(y), 's' + str(0) + str(y), link_delay_ms))
                print 'Adding link between switch ' + 's' + str(0) + str(y) + ' and ' + 's' + str(grid_x - 1) + str(y) + '\n\tRate: ' \
                        + str(link_Mbps) + ' Mbps\tDelay: ' + str(link_delay_ms)
                self.addLink(self.grid_routers[0][y], self.grid_routers[grid_x - 1][y], bw=link_Mbps, delay=str(link_delay_ms) + 'ms', max_queue_size=1000, use_htb=True)
    
    def get_controller_placement(self, latency_metric = LATENCY_METRIC_MIN_AVERAGE_DELAY):
        delay_metric_value = sys.float_info.max
        source_node_id = None

        for src_switch in self.routers:
            # Compute the shortest path tree for each possible controller placement
            nodes = set(self.routers)
            graph = defaultdict(list)
            for src,dst,cost in self.edges:
                graph[src].append((cost, dst))
         
            path_tree_map = defaultdict(lambda : None)
            queue, seen = [(0,src_switch,())], set()
            while queue:
                (cost,node1,path) = heappop(queue)
                if node1 not in seen:
                    seen.add(node1)
                    path = (cost, node1, path)
                    path_tree_map[node1] = path
         
                    for next_cost, node2 in graph.get(node1, ()):
                        if node2 not in seen:
                            heappush(queue, (cost + next_cost, node2, path))

            # Calculate the metric value for this position
            if latency_metric == LATENCY_METRIC_MIN_AVERAGE_DELAY:
                sum_delay = 0
                for receiver in path_tree_map:
                    sum_delay += path_tree_map[receiver][0]
                avg_delay = sum_delay / float(len(path_tree_map))
                if avg_delay < delay_metric_value:
                    source_node_id = src_switch
                    delay_metric_value = avg_delay
                    
            elif latency_metric == LATENCY_METRIC_MIN_MAXIMUM_DELAY:
                max_delay = 0
                for receiver in path_tree_map:
                    if path_tree_map[receiver][0] > max_delay:
                        max_delay = path_tree_map[receiver][0]
                if max_delay < delay_metric_value:
                    source_node_id = src_switch
                    delay_metric_value = max_delay
        
        print 'Found best controller placement at ' + str(source_node_id) + ' with metric: ' + str(delay_metric_value)
        return source_node_id, delay_metric_value
    
    def get_host_list(self):
        return self.hostnames
    
    def get_switch_list(self):
        return self.switch_names
    
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
        h0 = self.addHost('h0', ip='10.0.0.1')
        h1 = self.addHost('h1', ip='10.0.0.2')
        h2 = self.addHost('h2', ip='10.0.0.3')
        h3 = self.addHost('h3', ip='10.0.0.4')
        h4 = self.addHost('h4', ip='10.0.0.5')
        h5 = self.addHost('h5', ip='10.0.0.6')
        h6 = self.addHost('h6', ip='10.0.0.7')
        h7 = self.addHost('h7', ip='10.0.0.8')
        h8 = self.addHost('h8', ip='10.0.0.9')
        h9 = self.addHost('h9', ip='10.0.0.10')
        h10 = self.addHost('h10', ip='10.0.0.11')
        
        s0 = self.addSwitch('s0')
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')
        s5 = self.addSwitch('s5')
        s6 = self.addSwitch('s6')
        
        # Add links
        self.addLink(s0, s1, bw = 10, use_htb = True)
        self.addLink(s0, s2, bw = 10, use_htb = True)
        self.addLink(s1, s3, bw = 10, use_htb = True)
        self.addLink(s3, s4, bw = 10, use_htb = True)
        self.addLink(s1, s4, bw = 10, use_htb = True)
        self.addLink(s1, s5, bw = 10, use_htb = True)
        self.addLink(s5, s2, bw = 10, use_htb = True)
        self.addLink(s2, s6, bw = 10, use_htb = True)
        self.addLink(s6, s4, bw = 10, use_htb = True)
        
        self.addLink(s0, h0, bw = 10, use_htb = True)
        self.addLink(s2, h1, bw = 10, use_htb = True)
        self.addLink(s2, h2, bw = 10, use_htb = True)
        self.addLink(s4, h3, bw = 10, use_htb = True)
        self.addLink(s4, h4, bw = 10, use_htb = True)
        self.addLink(s4, h5, bw = 10, use_htb = True)
        self.addLink(s1, h6, bw = 10, use_htb = True)
        self.addLink(s5, h7, bw = 10, use_htb = True)
        self.addLink(s6, h8, bw = 10, use_htb = True)
        self.addLink(s3, h9, bw = 10, use_htb = True)
        self.addLink(s1, h10, bw = 10, use_htb = True)

    def mcastConfig(self, net):
        # Configure hosts for multicast support
        net.get('h0').cmd('route add -net 224.0.0.0/4 h0-eth0')
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
    
    def get_host_list(self):
        return ['h0', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'h8', 'h9', 'h10']
    
    def get_switch_list(self):
        return ['s0', 's1', 's2', 's3', 's4', 's5', 's6']