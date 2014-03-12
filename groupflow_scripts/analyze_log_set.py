#!/usr/bin/env python
import sys

class MulticastGroupStatRecord(object):
    def __init__(self, group_index, num_receivers, num_flows, max_link_mbps, avg_link_mbps, traffic_conc, link_mbps_std_dev, flow_tracker_response_time, flow_tracker_network_time, flow_tracker_processing_time):
        self.group_index = group_index
        self.num_receivers = num_receivers
        self.num_flows = num_flows
        self.max_link_mbps = max_link_mbps
        self.avg_link_mbps = avg_link_mbps
        self.traffic_conc = traffic_conc
        self.link_mbps_std_dev = link_mbps_std_dev
        self.flow_tracker_response_time = flow_tracker_response_time
        self.flow_tracker_network_time = flow_tracker_network_time
        self.flow_tracker_processing_time = flow_tracker_processing_time

def mean_confidence_interval(data, confidence=0.95):
	import scipy.stats
	from numpy import mean, array, sqrt 
	a = 1.0 * array(data)
	n = len(a)
	m, se = mean(a), scipy.stats.sem(a)
	# calls the inverse CDF of the Student's t distribution
	h = se * scipy.stats.t._ppf((1+confidence)/2., n-1)
	return m-h, m+h
    

def read_log_set(filepath_prefix, num_logs, output_filepath):
    group_records = []  # list of lists -> group_records[group_index] = list of MulticastGroupStatRecords
    num_groups_list = []
    
    for log_index in range(0, num_logs):
        filepath = filepath_prefix + str(log_index) + '.log'
        log_file = open(filepath, 'r')
        num_groups = 0
        for line in log_file:
            if 'Group:' in line:
                #print line,
                num_groups += 1
                split_line = line.split(' ')
                group_index = int(split_line[0][len('Group:'):])
                num_receivers = int(split_line[1][len('NumReceivers:'):])
                num_flows = int(split_line[2][len('TotalNumFlows:'):])
                max_link_mbps = float(split_line[3][len('MaxLinkUsageMbps:'):])
                avg_link_mbps = float(split_line[4][len('AvgLinkUsageMbps:'):])
                traffic_conc = float(split_line[5][len('TrafficConcentration:'):])
                link_mbps_std_dev = float(split_line[6][len('LinkUsageStdDev:'):])
                flow_tracker_response_time = float(split_line[7][len('ResponseTime:'):])
                flow_tracker_network_time = float(split_line[8][len('NetworkTime:'):])
                flow_tracker_processing_time = float(split_line[9][len('ProcessingTime:'):])
                group_record = MulticastGroupStatRecord(group_index, num_receivers, num_flows, max_link_mbps, avg_link_mbps, traffic_conc, link_mbps_std_dev, flow_tracker_response_time, flow_tracker_network_time, flow_tracker_processing_time)
                
                if group_index < len(group_records):
                    group_records[group_index].append(group_record)
                else:
                    group_records.append([group_record])
        num_groups_list.append(num_groups)
        log_file.close()
        print 'Processed log: ' + str(filepath)
        
    return group_records, num_groups_list


def print_group_record_statistics(group_records, num_groups_list):
    avg_num_groups_supported = float(sum(num_groups_list)) / len(num_groups_list)
    ci_upper, ci_lower = mean_confidence_interval(num_groups_list)
    print 'Average # Groups Supported: ' + str(avg_num_groups_supported) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
    print 'num_groups = ' + str(avg_num_groups_supported) + ';'
    print 'num_groups_ci = ' + str(abs(ci_upper - ci_lower) / 2) + ';'
    print ' '
    
    traffic_conc_avgs = []
    traffic_conc_cis = []
    link_std_dev_avgs = []
    link_std_dev_cis = []
    link_avg_mbps_avgs = []
    link_avg_mbps_cis = []
    num_flows_avgs = []
    num_flows_cis = []
    response_time_avgs = []
    response_time_cis = []
    network_time_avgs = []
    network_time_cis = []
    processing_time_avgs = []
    processing_time_cis = []
    
    for group_index in range(0, len(group_records)):
        if len(group_records[group_index]) < 2:
            # Ignore entries that do not produce valid confidence intervals
            continue
            
        print 'Group #' + str(group_index) + ' stats:'
        print '# Trials:\t\t' + str(len(group_records[group_index]))
        
        num_receivers_list = [float(r.num_receivers) for r in group_records[group_index]]
        avg = sum(num_receivers_list) / len(num_receivers_list)
        ci_upper, ci_lower = mean_confidence_interval(num_receivers_list)
        print 'NumReceivers:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        num_flows_list = [float(r.num_flows) for r in group_records[group_index]]
        avg = sum(num_flows_list) / len(num_flows_list)
        num_flows_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(num_flows_list)
        num_flows_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'TotalNumFlows:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'

        max_link_mbps_list = [float(r.max_link_mbps) for r in group_records[group_index]]
        avg = sum(max_link_mbps_list) / len(max_link_mbps_list)
        ci_upper, ci_lower = mean_confidence_interval(max_link_mbps_list)
        print 'MaxLinkUsageMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        avg_link_mbps_list = [float(r.avg_link_mbps) for r in group_records[group_index]]
        avg = sum(avg_link_mbps_list) / len(avg_link_mbps_list)
        link_avg_mbps_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(avg_link_mbps_list)
        link_avg_mbps_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'AvgLinkUsageMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        traffic_conc_list = [float(r.traffic_conc) for r in group_records[group_index]]
        avg = sum(traffic_conc_list) / len(traffic_conc_list)
        traffic_conc_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(traffic_conc_list)
        traffic_conc_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'TrafficConcentration:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        link_mbps_std_dev_list = [float(r.link_mbps_std_dev) for r in group_records[group_index]]
        avg = sum(link_mbps_std_dev_list) / len(link_mbps_std_dev_list)
        link_std_dev_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(link_mbps_std_dev_list)
        link_std_dev_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'LinkUsageStdDev:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        response_time_list = [float(r.flow_tracker_response_time) for r in group_records[group_index]]
        avg = sum(response_time_list) / len(response_time_list)
        response_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(response_time_list)
        response_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'ResponseTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        network_time_list = [float(r.flow_tracker_network_time) for r in group_records[group_index]]
        avg = sum(network_time_list) / len(network_time_list)
        network_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(network_time_list)
        network_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'NetworkTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        processing_time_list = [float(r.flow_tracker_processing_time) for r in group_records[group_index]]
        avg = sum(processing_time_list) / len(processing_time_list)
        processing_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(processing_time_list)
        processing_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'ProcessingTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
    print ' '
    print 'traffic_conc = [' + ', '.join([str(r) for r in traffic_conc_avgs]) + '];'
    print 'traffic_conc_ci = [' + ', '.join([str(r) for r in traffic_conc_cis]) + '];'
    print 'link_std_dev = [' + ', '.join([str(r) for r in link_std_dev_avgs]) + '];'
    print 'link_std_dev_ci = [' + ', '.join([str(r) for r in link_std_dev_cis]) + '];'
    print 'link_avg_mbps = [' + ', '.join([str(r) for r in link_avg_mbps_avgs]) + '];'
    print 'link_avg_mbps_ci = [' + ', '.join([str(r) for r in link_avg_mbps_cis]) + '];'
    print 'num_flows = [' + ', '.join([str(r) for r in num_flows_avgs]) + '];'
    print 'num_flows_ci = [' + ', '.join([str(r) for r in num_flows_cis]) + '];'
    print 'response_time = [' + ', '.join([str(r) for r in response_time_avgs]) + '];'
    print 'response_time_ci = [' + ', '.join([str(r) for r in response_time_cis]) + '];'
    print 'network_time = [' + ', '.join([str(r) for r in network_time_avgs]) + '];'
    print 'network_time_ci = [' + ', '.join([str(r) for r in network_time_cis]) + '];'
    print 'processing_time = [' + ', '.join([str(r) for r in processing_time_avgs]) + '];'
    print 'processing_time_ci = [' + ', '.join([str(r) for r in processing_time_cis]) + '];'

if __name__ == '__main__':
    if len(sys.argv) >= 4:
        filepath_prefix = sys.argv[1]
        num_logs = int(sys.argv[2])
        output_filepath = sys.argv[3]
        group_records, num_groups_list = read_log_set(filepath_prefix, num_logs, output_filepath)
        print_group_record_statistics(group_records, num_groups_list)
    