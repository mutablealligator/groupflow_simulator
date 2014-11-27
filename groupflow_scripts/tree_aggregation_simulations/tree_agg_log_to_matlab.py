#!/usr/bin/env python
import sys

def list_to_matlab_str(list):
    matlab_str = '['
    first_value = True
    for value in list:
        if first_value:
            matlab_str = matlab_str + str(value)
            first_value = False
        else:
            matlab_str = matlab_str + ', ' + str(value)
    matlab_str = matlab_str + '];'
    return matlab_str

def write_log_stats_to_file(prefix, bw_overhead_list, ft_reduction_list, runtime_list, num_trees_list, output_filepath):
    if len(bw_overhead_list) == 0:
        # Log set is empty
        return
    
    output_file = open(output_filepath, 'a')
    output_file.write(prefix + 'bw_overhead = ' + list_to_matlab_str(bw_overhead_list) + '\n')
    output_file.write(prefix + 'ft_reduction = ' + list_to_matlab_str(ft_reduction_list) + '\n')
    output_file.write(prefix + 'runtime = ' + list_to_matlab_str(runtime_list) + '\n')
    output_file.write(prefix + 'num_trees = ' + list_to_matlab_str(num_trees_list) + '\n')
    output_file.close()

def read_tree_agg_log_set(log_filepath, output_filepath):
    cur_log_set_prefix = ''
    
    bw_overhead_list = []
    ft_reduction_list = []
    runtime_list = []
    num_trees_list = []
    
    log_file = open(log_filepath, 'r')
    for line_raw in log_file:
        line = line_raw.strip()
        
        if line.startswith('Log Set: '):
            # Start of new log set
            # Write any currently stored stats to file
            write_log_stats_to_file(cur_log_set_prefix, bw_overhead_list, ft_reduction_list, runtime_list, num_trees_list, output_filepath)
            
            # Clear all stored data
            bw_overhead_list = []
            ft_reduction_list = []
            runtime_list = []
            num_trees_list = []
            
            # Get the new log set prefix
            cur_log_set_prefix = line[len('Log Set: '):]
        
        if line.startswith('Average Bandwidth Overhead: '):
            bw_overhead_list.append(float(line[len('Average Bandwidth Overhead: '):]))
            
        if line.startswith('Average Flow Table Reduction: '):
            ft_reduction_list.append(float(line[len('Average Flow Table Reduction: '):]))
        
        if line.startswith('Average # Aggregated Trees: '):
            num_trees_list.append(float(line[len('Average # Aggregated Trees: '):]))
            
        if line.startswith('Average Tree Agg. Run-Time: '):
            runtime_list.append(float(line[len('Average Tree Agg. Run-Time: '):]))
    
    # Write out stats for the final log set
    write_log_stats_to_file(cur_log_set_prefix, bw_overhead_list, ft_reduction_list, runtime_list, num_trees_list, output_filepath)
    log_file.close()

    
if __name__ == '__main__':
    if len(sys.argv) >= 3:
        input_log_filepath = sys.argv[1]
        output_filepath = sys.argv[2]
        read_tree_agg_log_set(input_log_filepath, output_filepath)
    