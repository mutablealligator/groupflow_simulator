#!/usr/bin/env python
import sys

class DynamicMulticastStatRecord(object):
    def __init__(self, time_index, sim_time, num_flows, max_link_mbps, avg_link_mbps, traffic_conc, link_mbps_std_dev, flow_tracker_response_time, flow_tracker_network_time, flow_tracker_processing_time, switch_load_mbps):
        self.time_index = time_index
        self.sim_time = sim_time
        self.num_flows = num_flows
        self.max_link_mbps = max_link_mbps
        self.avg_link_mbps = avg_link_mbps
        self.traffic_conc = traffic_conc
        self.link_mbps_std_dev = link_mbps_std_dev
        self.flow_tracker_response_time = flow_tracker_response_time
        self.flow_tracker_network_time = flow_tracker_network_time
        self.flow_tracker_processing_time = flow_tracker_processing_time
        self.switch_load_mbps = switch_load_mbps

def mean_confidence_interval(data, confidence=0.95):
	import scipy.stats
	from numpy import mean, array, sqrt 
	a = 1.0 * array(data)
	n = len(a)
	m, se = mean(a), scipy.stats.sem(a)
	# calls the inverse CDF of the Student's t distribution
	h = se * scipy.stats.t._ppf((1+confidence)/2., n-1)
	return m-h, m+h
    

def read_dynamic_log_set(filepath_prefix, num_logs):
    stat_records = []  # list of lists -> stat_records[time_index] = list of DynamicMulticastStatRecords
    packet_loss_list = []
    
    for log_index in range(0, num_logs):
        filepath = filepath_prefix + str(log_index) + '.log'
        log_file = open(filepath, 'r')
        for line in log_file:
            if 'AvgPacketLoss:' in line:
                split_line = line.strip().split(' ')
                packet_loss = float(split_line[2][len('AvgPacketLoss:'):])
                packet_loss_list.append(packet_loss)
                
            if 'TimeIndex:' in line:
                split_line = line.strip().split(' ')
                time_index = int(split_line[0][len('TimeIndex:'):])
                sim_time = float(split_line[1][len('SimTime:'):])
                num_flows = int(split_line[2][len('TotalNumFlows:'):])
                max_link_mbps = float(split_line[3][len('MaxLinkUsageMbps:'):])
                avg_link_mbps = float(split_line[4][len('AvgLinkUsageMbps:'):])
                traffic_conc = float(split_line[5][len('TrafficConcentration:'):])
                link_mbps_std_dev = float(split_line[6][len('LinkUsageStdDev:'):])
                flow_tracker_response_time = float(split_line[7][len('ResponseTime:'):])
                flow_tracker_network_time = float(split_line[8][len('NetworkTime:'):])
                flow_tracker_processing_time = float(split_line[9][len('ProcessingTime:'):])
                switch_load_mbps = float(split_line[10][len('SwitchAvgLoadMbps:'):])
                group_record = DynamicMulticastStatRecord(time_index, sim_time, num_flows, max_link_mbps, avg_link_mbps, traffic_conc, link_mbps_std_dev, flow_tracker_response_time, flow_tracker_network_time, flow_tracker_processing_time, switch_load_mbps)
                
                if time_index < len(stat_records):
                    stat_records[time_index].append(group_record)
                else:
                    stat_records.append([group_record])
        
        log_file.close()
        print 'Processed log: ' + str(filepath)
    
    print 'Packet Loss Results:'
    print packet_loss_list
    return stat_records, packet_loss_list


def print_dynamic_trial_statistics(stat_records, output_prefix, packet_loss_list):    
    traffic_conc_avgs = []
    traffic_conc_cis = []
    link_std_dev_avgs = []
    link_std_dev_cis = []
    link_avg_mbps_avgs = []
    link_avg_mbps_cis = []
    link_max_mbps_avgs = []
    link_max_mbps_cis = []
    switch_load_mbps_avgs = []
    switch_load_mbps_cis = []
    num_flows_avgs = []
    num_flows_cis = []
    response_time_avgs = []
    response_time_cis = []
    network_time_avgs = []
    network_time_cis = []
    processing_time_avgs = []
    processing_time_cis = []
    
    print ' '
    
    for time_index in range(0, len(stat_records)):
        if len(stat_records[time_index]) < 2:
            # Ignore entries that do not produce valid confidence intervals
            continue
            
        print 'TimeIndex #' + str(time_index) + ' stats:'
        print '# Trials:\t\t' + str(len(stat_records[time_index]))
        
        # num_receivers_list = [float(r.num_receivers) for r in stat_records[time_index]]
        # avg = sum(num_receivers_list) / len(num_receivers_list)
        # ci_upper, ci_lower = mean_confidence_interval(num_receivers_list)
        # print 'NumReceivers:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        num_flows_list = [float(r.num_flows) for r in stat_records[time_index]]
        avg = sum(num_flows_list) / len(num_flows_list)
        num_flows_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(num_flows_list)
        num_flows_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'TotalNumFlows:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'

        max_link_mbps_list = [float(r.max_link_mbps) for r in stat_records[time_index]]
        avg = sum(max_link_mbps_list) / len(max_link_mbps_list)
        link_max_mbps_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(max_link_mbps_list)
        link_max_mbps_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'MaxLinkUsageMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        avg_link_mbps_list = [float(r.avg_link_mbps) for r in stat_records[time_index]]
        avg = sum(avg_link_mbps_list) / len(avg_link_mbps_list)
        link_avg_mbps_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(avg_link_mbps_list)
        link_avg_mbps_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'AvgLinkUsageMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        switch_load_mbps_list = [float(r.switch_load_mbps) for r in stat_records[time_index]]
        avg = sum(switch_load_mbps_list) / len(switch_load_mbps_list)
        switch_load_mbps_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(switch_load_mbps_list)
        switch_load_mbps_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'SwitchLoadMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        traffic_conc_list = [float(r.traffic_conc) for r in stat_records[time_index]]
        avg = sum(traffic_conc_list) / len(traffic_conc_list)
        traffic_conc_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(traffic_conc_list)
        traffic_conc_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'TrafficConcentration:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        link_mbps_std_dev_list = [float(r.link_mbps_std_dev) for r in stat_records[time_index]]
        avg = sum(link_mbps_std_dev_list) / len(link_mbps_std_dev_list)
        link_std_dev_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(link_mbps_std_dev_list)
        link_std_dev_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'LinkUsageStdDev:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        response_time_list = [float(r.flow_tracker_response_time) for r in stat_records[time_index]]
        avg = sum(response_time_list) / len(response_time_list)
        response_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(response_time_list)
        response_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'ResponseTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        network_time_list = [float(r.flow_tracker_network_time) for r in stat_records[time_index]]
        avg = sum(network_time_list) / len(network_time_list)
        network_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(network_time_list)
        network_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'NetworkTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        processing_time_list = [float(r.flow_tracker_processing_time) for r in stat_records[time_index]]
        avg = sum(processing_time_list) / len(processing_time_list)
        processing_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(processing_time_list)
        processing_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'ProcessingTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        print ' '
    
    # Print output in MATLAB matrix format
    avg_packet_loss = float(sum(packet_loss_list)) / len(packet_loss_list)
    ci_upper, ci_lower = mean_confidence_interval(packet_loss_list)
    packet_loss_ci = abs(ci_upper - ci_lower) / 2
    
    print str(output_prefix) + 'packet_loss = ' + str(avg_packet_loss) + ';'
    print str(output_prefix) + 'packet_loss_ci = ' + str(packet_loss_ci) + ';'
    print str(output_prefix) + 'traffic_conc = [' + ', '.join([str(r) for r in traffic_conc_avgs]) + '];'
    print str(output_prefix) + 'traffic_conc_ci = [' + ', '.join([str(r) for r in traffic_conc_cis]) + '];'
    print str(output_prefix) + 'link_std_dev = [' + ', '.join([str(r) for r in link_std_dev_avgs]) + '];'
    print str(output_prefix) + 'link_std_dev_ci = [' + ', '.join([str(r) for r in link_std_dev_cis]) + '];'
    print str(output_prefix) + 'link_avg_mbps = [' + ', '.join([str(r) for r in link_avg_mbps_avgs]) + '];'
    print str(output_prefix) + 'link_avg_mbps_ci = [' + ', '.join([str(r) for r in link_avg_mbps_cis]) + '];'
    print str(output_prefix) + 'link_max_mbps = [' + ', '.join([str(r) for r in link_avg_mbps_avgs]) + '];'
    print str(output_prefix) + 'link_max_mbps_ci = [' + ', '.join([str(r) for r in link_avg_mbps_cis]) + '];'
    print str(output_prefix) + 'switch_load_mbps = [' + ', '.join([str(r) for r in switch_load_mbps_avgs]) + '];'
    print str(output_prefix) + 'switch_load_mbps_ci = [' + ', '.join([str(r) for r in switch_load_mbps_cis]) + '];'
    print str(output_prefix) + 'num_flows = [' + ', '.join([str(r) for r in num_flows_avgs]) + '];'
    print str(output_prefix) + 'num_flows_ci = [' + ', '.join([str(r) for r in num_flows_cis]) + '];'
    print str(output_prefix) + 'response_time = [' + ', '.join([str(r) for r in response_time_avgs]) + '];'
    print str(output_prefix) + 'response_time_ci = [' + ', '.join([str(r) for r in response_time_cis]) + '];'
    print str(output_prefix) + 'network_time = [' + ', '.join([str(r) for r in network_time_avgs]) + '];'
    print str(output_prefix) + 'network_time_ci = [' + ', '.join([str(r) for r in network_time_cis]) + '];'
    print str(output_prefix) + 'processing_time = [' + ', '.join([str(r) for r in processing_time_avgs]) + '];'
    print str(output_prefix) + 'processing_time_ci = [' + ', '.join([str(r) for r in processing_time_cis]) + '];'

if __name__ == '__main__':
    if len(sys.argv) >= 4:
        filepath_prefix = sys.argv[1]
        num_logs = int(sys.argv[2])
        output_prefix = sys.argv[3]
        stat_records, packet_loss_list = read_dynamic_log_set(filepath_prefix, num_logs)
        print_dynamic_trial_statistics(stat_records, output_prefix, packet_loss_list)
    