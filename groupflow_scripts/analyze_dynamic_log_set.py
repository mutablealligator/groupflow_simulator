#!/usr/bin/env python
import sys

class DynamicMulticastFullTrialRecord(object):
    def __init__(self, packet_loss, avg_num_flows, avg_max_link_mbps, avg_avg_link_mbps, avg_traffic_conc, avg_link_mbps_std_dev, avg_flow_tracker_response_time,
            avg_flow_tracker_network_time, avg_flow_tracker_processing_time, avg_switch_load_mbps, avg_num_active_receivers):
        self.packet_loss = packet_loss
        self.avg_num_flows = avg_num_flows
        self.avg_max_link_mbps = avg_max_link_mbps
        self.avg_avg_link_mbps = avg_avg_link_mbps
        self.avg_traffic_conc = avg_traffic_conc
        self.avg_link_mbps_std_dev = avg_link_mbps_std_dev
        self.avg_flow_tracker_response_time = avg_flow_tracker_response_time
        self.avg_flow_tracker_network_time = avg_flow_tracker_network_time
        self.avg_flow_tracker_processing_time = avg_flow_tracker_processing_time
        self.avg_switch_load_mbps = avg_switch_load_mbps
        self.avg_num_active_receivers = avg_num_active_receivers
        
class DynamicMulticastTimeSnapshotRecord(object):
    def __init__(self, time_index, sim_time, num_flows, max_link_mbps, avg_link_mbps, traffic_conc, link_mbps_std_dev, flow_tracker_response_time, 
            flow_tracker_network_time, flow_tracker_processing_time, switch_load_mbps, num_active_receivers):
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
        self.num_active_receivers = num_active_receivers

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
    snapshot_records = []  # list of lists -> snapshot_records[time_index] = list of DynamicMulticastTimeSnapshotRecord
    full_trial_records = [] # List of DynamicMulticastFullTrialRecord
    
    for log_index in range(0, num_logs):
        filepath = filepath_prefix + str(log_index) + '.log'
        log_file = open(filepath, 'r')
        snapshots_cur_log = []
        packet_loss = 0
        valid_log = True
        
        for line in log_file:
            
            if 'AvgPacketLoss:' in line:
                split_line = line.strip().split(' ')
                packet_loss = float(split_line[2][len('AvgPacketLoss:'):])
                if packet_loss > 20:
                    # This occurs when the controller's LLDP polling incorrectly detects a link as down
                    # due to congestion on the link. When this occurs, all multicast routes are recalculated,
                    # and sometimes the particular link that was detected as down bisects the network, preventing
                    # multicast delivery and resulting in high packet loss.
                    print 'WARNING: DISCARDING OUTLIER WITH EXCESSIVE PACKET LOSS'
                    valid_log = False
                    break
            
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
                num_active_receivers = int(split_line[11][len('NumActiveReceivers:'):])
                snapshot_record = DynamicMulticastTimeSnapshotRecord(time_index, sim_time, num_flows, max_link_mbps, avg_link_mbps, traffic_conc, 
                        link_mbps_std_dev, flow_tracker_response_time, flow_tracker_network_time, flow_tracker_processing_time, switch_load_mbps,
                        num_active_receivers)
                
                if time_index < len(snapshot_records):
                    snapshot_records[time_index].append(snapshot_record)
                else:
                    snapshot_records.append([snapshot_record])
                    
                snapshots_cur_log.append(snapshot_record)
            
        if valid_log:
            avg_num_flows = float(sum(snapshot_record.num_flows for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_max_link_mbps = float(sum(snapshot_record.max_link_mbps for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_avg_link_mbps = float(sum(snapshot_record.avg_link_mbps for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_traffic_conc = float(sum(snapshot_record.traffic_conc for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_link_mbps_std_dev = float(sum(snapshot_record.link_mbps_std_dev for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_flow_tracker_response_time = float(sum(snapshot_record.flow_tracker_response_time for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_flow_tracker_network_time = float(sum(snapshot_record.flow_tracker_network_time for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_flow_tracker_processing_time = float(sum(snapshot_record.flow_tracker_processing_time for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_switch_load_mbps = float(sum(snapshot_record.switch_load_mbps for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            avg_num_active_receivers = float(sum(snapshot_record.num_active_receivers for snapshot_record in snapshots_cur_log)) / len(snapshots_cur_log)
            full_trial_record = DynamicMulticastFullTrialRecord(packet_loss, avg_num_flows, avg_max_link_mbps, avg_avg_link_mbps, avg_traffic_conc, avg_link_mbps_std_dev,
                    avg_flow_tracker_response_time, avg_flow_tracker_network_time, avg_flow_tracker_processing_time, avg_switch_load_mbps, avg_num_active_receivers)
            
            full_trial_records.append(full_trial_record)
                
        log_file.close()
        print 'Processed log: ' + str(filepath)
    
    return snapshot_records, full_trial_records


def print_dynamic_trial_statistics(snapshot_records, full_trial_records, output_prefix):    
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
    active_receivers_avgs = []
    active_receivers_cis = []
    
    print ' '
    
    for time_index in range(0, len(snapshot_records)):
        if len(snapshot_records[time_index]) < 2:
            # Ignore entries that do not produce valid confidence intervals
            continue
            
        print 'TimeIndex #' + str(time_index) + ' stats:'
        print '# Trials:\t\t' + str(len(snapshot_records[time_index]))
        
        # num_receivers_list = [float(r.num_receivers) for r in snapshot_records[time_index]]
        # avg = sum(num_receivers_list) / len(num_receivers_list)
        # ci_upper, ci_lower = mean_confidence_interval(num_receivers_list)
        # print 'NumReceivers:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        num_flows_list = [float(r.num_flows) for r in snapshot_records[time_index]]
        avg = sum(num_flows_list) / len(num_flows_list)
        num_flows_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(num_flows_list)
        num_flows_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'TotalNumFlows:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'

        max_link_mbps_list = [float(r.max_link_mbps) for r in snapshot_records[time_index]]
        avg = sum(max_link_mbps_list) / len(max_link_mbps_list)
        link_max_mbps_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(max_link_mbps_list)
        link_max_mbps_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'MaxLinkUsageMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        avg_link_mbps_list = [float(r.avg_link_mbps) for r in snapshot_records[time_index]]
        avg = sum(avg_link_mbps_list) / len(avg_link_mbps_list)
        link_avg_mbps_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(avg_link_mbps_list)
        link_avg_mbps_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'AvgLinkUsageMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        switch_load_mbps_list = [float(r.switch_load_mbps) for r in snapshot_records[time_index]]
        avg = sum(switch_load_mbps_list) / len(switch_load_mbps_list)
        switch_load_mbps_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(switch_load_mbps_list)
        switch_load_mbps_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'SwitchLoadMbps:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        traffic_conc_list = [float(r.traffic_conc) for r in snapshot_records[time_index]]
        avg = sum(traffic_conc_list) / len(traffic_conc_list)
        traffic_conc_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(traffic_conc_list)
        traffic_conc_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'TrafficConcentration:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        link_mbps_std_dev_list = [float(r.link_mbps_std_dev) for r in snapshot_records[time_index]]
        avg = sum(link_mbps_std_dev_list) / len(link_mbps_std_dev_list)
        link_std_dev_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(link_mbps_std_dev_list)
        link_std_dev_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'LinkUsageStdDev:\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        response_time_list = [float(r.flow_tracker_response_time) for r in snapshot_records[time_index]]
        avg = sum(response_time_list) / len(response_time_list)
        response_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(response_time_list)
        response_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'ResponseTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        network_time_list = [float(r.flow_tracker_network_time) for r in snapshot_records[time_index]]
        avg = sum(network_time_list) / len(network_time_list)
        network_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(network_time_list)
        network_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'NetworkTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        processing_time_list = [float(r.flow_tracker_processing_time) for r in snapshot_records[time_index]]
        avg = sum(processing_time_list) / len(processing_time_list)
        processing_time_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(processing_time_list)
        processing_time_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'ProcessingTime:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        
        num_active_receivers_list = [float(r.num_active_receivers) for r in snapshot_records[time_index]]
        avg = sum(num_active_receivers_list) / len(num_active_receivers_list)
        active_receivers_avgs.append(avg)
        ci_upper, ci_lower = mean_confidence_interval(num_active_receivers_list)
        active_receivers_cis.append(abs(ci_upper - ci_lower) / 2)
        print 'ActiveReceivers:\t\t' + str(avg) + '\t[' + str(ci_lower) + ', ' + str(ci_upper) + ']'
        print ' '
    
    # Print time indexed output in MATLAB matrix format
    print str(output_prefix) + 'traffic_conc = [' + ', '.join([str(r) for r in traffic_conc_avgs]) + '];'
    print str(output_prefix) + 'traffic_conc_ci = [' + ', '.join([str(r) for r in traffic_conc_cis]) + '];'
    print str(output_prefix) + 'link_std_dev = [' + ', '.join([str(r) for r in link_std_dev_avgs]) + '];'
    print str(output_prefix) + 'link_std_dev_ci = [' + ', '.join([str(r) for r in link_std_dev_cis]) + '];'
    print str(output_prefix) + 'link_avg_mbps = [' + ', '.join([str(r) for r in link_avg_mbps_avgs]) + '];'
    print str(output_prefix) + 'link_avg_mbps_ci = [' + ', '.join([str(r) for r in link_avg_mbps_cis]) + '];'
    print str(output_prefix) + 'link_max_mbps = [' + ', '.join([str(r) for r in link_max_mbps_avgs]) + '];'
    print str(output_prefix) + 'link_max_mbps_ci = [' + ', '.join([str(r) for r in link_max_mbps_avgs]) + '];'
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
    print str(output_prefix) + 'active_receivers = [' + ', '.join([str(r) for r in active_receivers_avgs]) + '];'
    print str(output_prefix) + 'active_receivers_ci = [' + ', '.join([str(r) for r in active_receivers_cis]) + '];'
    
    print ' '
    
    # Print overall trial averages in MATLAB format
    avg = float(sum(trial_record.packet_loss for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.packet_loss for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_packet_loss = ' + str(avg) + ';'
    print str(output_prefix) + 'fulltrial_packet_loss_ci = ' + str(ci) + ';'
    
    avg = float(sum(trial_record.avg_traffic_conc for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.avg_traffic_conc for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_traffic_conc = '  + str(avg)
    print str(output_prefix) + 'fulltrial_traffic_conc_ci = '  + str(ci)
    
    avg = float(sum(trial_record.avg_link_mbps_std_dev for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.avg_link_mbps_std_dev for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_link_std_dev = ' + str(avg)
    print str(output_prefix) + 'fulltrial_link_std_dev_ci = ' + str(ci)
    
    avg = float(sum(trial_record.avg_avg_link_mbps for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.avg_avg_link_mbps for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_link_avg_mbps = ' + str(avg)
    print str(output_prefix) + 'fulltrial_link_avg_mbps_ci = ' + str(ci)

    avg = float(sum(trial_record.avg_max_link_mbps for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.avg_max_link_mbps for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_link_max_mbps = ' + str(avg)
    print str(output_prefix) + 'fulltrial_link_max_mbps_ci = ' + str(ci)
    
    avg = float(sum(trial_record.avg_switch_load_mbps for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.avg_switch_load_mbps for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_switch_load_mbps = ' + str(avg)
    print str(output_prefix) + 'fulltrial_switch_load_mbps_ci = ' + str(ci)
    
    avg = float(sum(trial_record.avg_num_flows for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.avg_num_flows for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_num_flows = ' + str(avg)
    print str(output_prefix) + 'fulltrial_num_flows_ci = ' + str(ci)
    
    avg = float(sum(trial_record.avg_num_active_receivers for trial_record in full_trial_records)) / len(full_trial_records)
    ci_upper, ci_lower = mean_confidence_interval([trial_record.avg_num_active_receivers for trial_record in full_trial_records])
    ci = abs(ci_upper - ci_lower) / 2
    print str(output_prefix) + 'fulltrial_num_active_receivers = ' + str(avg)
    print str(output_prefix) + 'fulltrial_num_active_receivers_ci = ' + str(ci)
    
    
if __name__ == '__main__':
    if len(sys.argv) >= 4:
        filepath_prefix = sys.argv[1]
        num_logs = int(sys.argv[2])
        output_prefix = sys.argv[3]
        snapshot_records, full_trial_records = read_dynamic_log_set(filepath_prefix, num_logs)
        print_dynamic_trial_statistics(snapshot_records, full_trial_records, output_prefix)
    