from scipy.stats import truncnorm, tstd, poisson, expon
from numpy.random import randint, uniform
from datetime import datetime
from collections import defaultdict
from sets import Set
from heapq import  heappop, heappush
from time import time
from scipy.cluster.hierarchy import *
from scipy.spatial.distance import pdist
import scipy.spatial.distance as ssd
import os
import sys
import signal
import numpy as np
import matplotlib.pyplot as plt

def get_cluster_group_aggregation(group_indexes, linkage_array, difference_threshold):
    group_map = {}
    for group_index in group_indexes:
        group_map[group_index] = [group_index]
    
    next_cluster_index = len(group_indexes)
    for cluster in linkage_array:
        if cluster[2] > difference_threshold:
            break
        
        new_cluster_list = []
        for index in group_map[cluster[0]]:
            new_cluster_list.append(index)
        for index in group_map[cluster[1]]:
            new_cluster_list.append(index)
        del group_map[cluster[0]]
        del group_map[cluster[1]]
        group_map[next_cluster_index] = new_cluster_list
        next_cluster_index += 1
    
    #print 'Group Aggregations for Difference Threshold: ' + str(difference_threshold)
    #for cluster_index in group_map:
    #    print 'Cluster Index: ' + str(cluster_index)
    #    for group_index in group_map[cluster_index]:
    #        print str(group_index) + ' ',
    #    print ' '
            
    return group_map

def calc_best_rendevouz_point(topology, mcast_groups):
    aggregated_group_nodes = []
    for group in mcast_groups:
        aggregated_group_nodes.append(group.src_node_id)
        for receiver_id in group.receiver_ids:
            aggregated_group_nodes.append(receiver_id)
    
    aggregated_group_nodes = list(Set(aggregated_group_nodes))
        
    min_sum_tree_length = sys.maxint
    rv_node_id = None
    for forwarding_element in topology.forwarding_elements:
        no_rendevouz_path = False
        potential_rv_node_id = forwarding_element.node_id
        sum_tree_length = len(topology.get_shortest_path_tree(potential_rv_node_id, aggregated_group_nodes))
            
        if sum_tree_length <= min_sum_tree_length:
            min_sum_tree_length = sum_tree_length
            rv_node_id = potential_rv_node_id
    
    return rv_node_id, aggregated_group_nodes

def aggregate_groups_via_tree_sim(topology, mcast_groups, bandwidth_overhead_threshold):
    group_map = defaultdict(lambda : None)
    next_agg_tree_index = 0
    
    for group in mcast_groups:
        if len(group_map) == 0:
            # This is the first group to initialize, always uses a native multicast tree
            group.rendevouz_point_node_id = group.src_node_id
            group.rendevouz_point_shortest_path = []
            group.aggregated_mcast_tree = topology.get_shortest_path_tree(group.src_node_id, list(group.receiver_ids))
            group.aggregated_mcast_tree_index = next_agg_tree_index
            group.aggregated_bandwidth_Mbps = len(group.aggregated_mcast_tree) * group.bandwidth_Mbps
            group_map[next_agg_tree_index] = [group.group_index]
            next_agg_tree_index += 1
            continue
        
        # If this is not the first group, iterate through all existing aggregated trees, and check if any can be extended
        # to cover the group without exceeding the bandwidth overhead threshold
        final_aggregated_groups = None
        final_aggregated_tree_index = None
        final_aggregated_mcast_tree = None
        final_rv_node_id = None
        final_aggregated_bandwidth_overhead = None
        
        for agg_tree_index in group_map:
            test_aggregated_groups = [group]
            for group_index in group_map[agg_tree_index]:
                test_aggregated_groups.append(mcast_groups[group_index])
                
            rv_node_id, aggregated_group_nodes = calc_best_rendevouz_point(topology, test_aggregated_groups)
            aggregated_mcast_tree = topology.get_shortest_path_tree(rv_node_id, aggregated_group_nodes)
            
            # Got a rendevouz node for this potential aggregation, now calculate the bandwidth overhead of this potential aggregation
            native_bandwidth_Mbps = 0
            aggregated_bandwidth_Mbps = 0
            for test_group in test_aggregated_groups:
                native_bandwidth_Mbps += test_group.native_bandwidth_Mbps
                aggregated_bandwidth_Mbps += (len(aggregated_mcast_tree) * test_group.bandwidth_Mbps)
            bandwidth_overhead_ratio = float(aggregated_bandwidth_Mbps) / native_bandwidth_Mbps;
                
            if bandwidth_overhead_ratio > bandwidth_overhead_threshold:
                continue    # This aggregation causes the bandwidth overhead ratio to exceed the threshold
            
            if final_aggregated_bandwidth_overhead is None or bandwidth_overhead_ratio < final_aggregated_bandwidth_overhead:
                final_aggregated_bandwidth_overhead = bandwidth_overhead_ratio
                final_aggregated_tree_index = agg_tree_index
                final_aggregated_groups = test_aggregated_groups
                final_aggregated_mcast_tree = aggregated_mcast_tree
                final_rv_node_id = rv_node_id
        
        # At this point, either a valid aggregation has been found (and stored in the "final" variables), or the group will
        # be assigned to a new, native tree
        if final_aggregated_tree_index is not None:
            # A valid aggregation has been found
            # print 'Assigning group #' + str(group.group_index) + ' to aggregated tree #' + str(final_aggregated_tree_index) + ' (BW Overhead: ' + str(final_aggregated_bandwidth_overhead) + ')'
            group_map[final_aggregated_tree_index].append(group.group_index)
            for agg_group in final_aggregated_groups:
                agg_group.rendevouz_point_node_id = final_rv_node_id
                agg_group.rendevouz_point_shortest_path = []
                agg_group.aggregated_mcast_tree = final_aggregated_mcast_tree
                agg_group.aggregated_mcast_tree_index = final_aggregated_tree_index
                agg_group.aggregated_bandwidth_Mbps = (len(agg_group.aggregated_mcast_tree) * agg_group.bandwidth_Mbps)
        else:
            # Create a new aggregated tree index for the group
            group.rendevouz_point_node_id = group.src_node_id
            group.rendevouz_point_shortest_path = []
            group.aggregated_mcast_tree = topology.get_shortest_path_tree(group.src_node_id, list(group.receiver_ids))
            group.aggregated_mcast_tree_index = next_agg_tree_index
            group.aggregated_bandwidth_Mbps = len(group.aggregated_mcast_tree) * group.bandwidth_Mbps
            group_map[next_agg_tree_index] = [group.group_index]
            next_agg_tree_index += 1
        
    # print 'Tree similarity aggregation results:\n' + str(group_map)
    return mcast_groups, group_map
        

def generate_cluster_aggregated_mcast_trees(topology, mcast_groups, group_map):
    for group_aggregation in group_map:
        # print 'Cluster #' + str(group_aggregation) + ' - Groups: ' + (str(group_map[group_aggregation]))
        cluster_groups = []
        for mcast_group_id in group_map[group_aggregation]:
            mcast_groups[mcast_group_id].aggregated_mcast_tree_index = group_aggregation
            cluster_groups.append(mcast_groups[mcast_group_id])
        
        min_sum_path_length = sys.maxint
        rv_node_id, aggregated_group_receivers = calc_best_rendevouz_point(topology, cluster_groups)
        
        for mcast_group_id in group_map[group_aggregation]:
            src_node_id = mcast_groups[mcast_group_id].src_node_id
            shortest_path = topology.get_shortest_path_tree(src_node_id, [rv_node_id])
            mcast_groups[mcast_group_id].rendevouz_point_node_id = rv_node_id
            mcast_groups[mcast_group_id].rendevouz_point_shortest_path = shortest_path
            mcast_groups[mcast_group_id].aggregated_mcast_tree = topology.get_shortest_path_tree(rv_node_id, aggregated_group_receivers)
            mcast_groups[mcast_group_id].aggregated_bandwidth_Mbps = ((len(mcast_groups[mcast_group_id].aggregated_mcast_tree) 
                    + len(mcast_groups[mcast_group_id].rendevouz_point_shortest_path)) * mcast_groups[mcast_group_id].bandwidth_Mbps)
    
    return mcast_groups, group_map

def get_terminal_vertices(edge_list):
    tail_set = Set()
    head_set = Set()
    for edge in edge_list:
        tail_set.add(edge[0])
        head_set.add(edge[1])
    
    return (tail_set.union(head_set)) - (head_set.intersection(tail_set))

def get_origin_vertices(edge_list, origin_candidates):
    node_edge_count = defaultdict(lambda : None)
    for edge in edge_list:
        if node_edge_count[edge[0]] is None:
            node_edge_count[edge[0]] = 1
        else:
            node_edge_count[edge[0]] = node_edge_count[edge[0]] + 1
        
        if node_edge_count[edge[1]] is None:
            node_edge_count[edge[1]] = -1
        else:
            node_edge_count[edge[1]] = node_edge_count[edge[1]] - 1
    
    origin_set = Set()
    for node_id in origin_candidates:
        if node_edge_count[node_id] is not None and node_edge_count[node_id] > 0:
            origin_set.add(node_id)
            
    return origin_set
        
    
def get_intermediate_vertices(edge_list):
    tail_set = Set()
    head_set = Set()
    for edge in edge_list:
        tail_set.add(edge[0])
        head_set.add(edge[1])
    
    return tail_set.intersection(head_set)

def aggregate_groups_via_clustering(groups, linkage_method, similarity_threshold, plot_dendrogram = False):
    src_dist_clustering = False
    if '_srcdist' in linkage_method:
        src_dist_clustering = True
        linkage_method = linkage_method[:-len('_srcdist')]
        
    # Generate the distance matrix used for clustering
    receivers_list = []
    for group in groups:
        receivers_list.append(list(group.receiver_ids))
    receivers_array = np.array(receivers_list)
    distance_matrix = []
    group_index = 0
    group_indexes = []
    for group1 in groups:
        distance_matrix.append([])
        for group2 in groups:
            jaccard_distance = group1.jaccard_distance(group2)
            if src_dist_clustering:
                src_distance = len(topo.get_shortest_path_tree(group1.src_node_id, [group2.src_node_id]))
                src_distance_ratio = (float(src_distance)/topo.network_diameter) # Distance between source nodes as a percentage of the network diameter
                distance_matrix[group_index].append(1 - ((1 - jaccard_distance) * (1 - src_distance_ratio)))
            else:
                distance_matrix[group_index].append(jaccard_distance)
            
        group_indexes.append(group_index)
        group_index += 1
        
    comp_dist_array = ssd.squareform(distance_matrix)
    
    # Perform clustering, and plot a dendrogram of the results if requested
    z = linkage(comp_dist_array, method=linkage_method)
    group_map = get_cluster_group_aggregation(group_indexes, z, similarity_threshold)
    
    if plot_dendrogram:
        plt.figure(1, figsize=(6, 5))
        print 'Linkage Array:\n' + str(z)
        print ' '
        d = dendrogram(z, show_leaf_counts=True)
        plt.title('Multicast Group Clustering')
        plt.xlabel('Multicast Group Index')
        plt.ylabel('Cluster Similarity')
        plt.show()
    
    # Generate aggregated multicast trees based on the generated clusters
    generate_cluster_aggregated_mcast_trees(topo, groups, group_map)
    
    return groups, group_map
    
def calc_network_performance_metrics(groups, group_map, debug_print = False):
    native_network_flow_table_size = 0
    aggregated_network_flow_table_size = 0
    
    native_bandwidth_Mbps = 0
    aggregated_bandwidth_Mbps = 0
    
    seen_aggregated_tree_indexes = []
    non_reducible_flow_table_entries = 0
    
    for group in groups:
        # print 'Calculating flow table size for group: ' + str(group.group_index)
        non_reducible_flow_table_entries += len(group.receiver_ids) + 1
        native_bandwidth_Mbps += group.native_bandwidth_Mbps
        aggregated_bandwidth_Mbps += group.aggregated_bandwidth_Mbps
        
        native_network_flow_table_size += len(group.native_mcast_tree) + 1
        aggregated_network_flow_table_size += len(group.receiver_ids) + 1
        
        #print 'Native flow table size: ' + str(len(group.native_mcast_tree) + 1)
        #print 'Aggregated non-reducible state: ' + str(len(group.receiver_ids) + 1)
        
        if group.aggregated_mcast_tree_index not in seen_aggregated_tree_indexes:
            #print 'Calculating flow table size for aggregated tree: ' + str(group.aggregated_mcast_tree_index)
            seen_aggregated_tree_indexes.append(group.aggregated_mcast_tree_index)
            tree_terminal_vertices = get_terminal_vertices(group.aggregated_mcast_tree)
            #print str(tree_terminal_vertices)
            aggregated_network_flow_table_size += len(group.aggregated_mcast_tree) + 1 - len(get_terminal_vertices(group.aggregated_mcast_tree))
            #print 'Aggregated reducible state: ' + str(len(group.aggregated_mcast_tree) + 1)
            
        
        #if debug_print:
        #    print ' '
        #    group.debug_print()
    
    reducible_native_network_flow_table_size = native_network_flow_table_size - non_reducible_flow_table_entries
    reducible_aggregated_network_flow_table_size = aggregated_network_flow_table_size - non_reducible_flow_table_entries
    
    bandwidth_overhead_ratio = float(aggregated_bandwidth_Mbps) / float(native_bandwidth_Mbps)
    flow_table_reduction_ratio = 1 - float(aggregated_network_flow_table_size) / float(native_network_flow_table_size)
    reducible_flow_table_reduction_ratio = 1 - float(reducible_aggregated_network_flow_table_size) / float(reducible_native_network_flow_table_size)
    
    if debug_print:
        print ' '
        print 'Aggregated Network Bandwidth Utilization: ' + str(aggregated_bandwidth_Mbps) + ' Mbps'
        print 'Native Network Bandwidth Utilization: ' + str(native_bandwidth_Mbps) + ' Mbps'
        print 'Bandwidth Overhead Ratio: ' + str(bandwidth_overhead_ratio)
        print ' '
        print 'Native Network Flow Table Size: ' + str(native_network_flow_table_size)
        print 'Aggregated Network Flow Table Size: ' + str(aggregated_network_flow_table_size)
        print 'Flow Table Reduction Ratio: ' + str(flow_table_reduction_ratio)
        print ' '
        print 'Reducible Native Network Flow Table Size: ' + str(reducible_native_network_flow_table_size)
        print 'Reducible Aggregated Network Flow Table Size: ' + str(reducible_aggregated_network_flow_table_size)
        print 'Reducible Flow Table Reduction Ratio: ' + str(reducible_flow_table_reduction_ratio)
    
    return bandwidth_overhead_ratio, flow_table_reduction_ratio, reducible_flow_table_reduction_ratio, len(group_map)
    
    
class ForwardingElement(object):
    def __init__(self, node_id):
        self.node_id = node_id
    
    def __str__(self):
        return 'Forwarding Element #' + str(self.node_id)

        
class Link(object):
    def __init__(self, tail_node_id, head_node_id, cost):
        self.tail_node_id = tail_node_id    # Node ID from which the link originates
        self.head_node_id = head_node_id    # Node ID to which the link delivers traffic
        self.cost = cost
    
    def __str__(self):
        return 'Link: ' + str(self.tail_node_id) + ' --> ' + str(self.head_node_id) + ' C:' + str(self.cost)

        
class SimTopo(object):
    def __init__(self):
        self.forwarding_elements = []
        self.links = []
        self.shortest_path_map = defaultdict(lambda : None)
        self.network_diameter = 0
        self.recalc_path_tree_map = True
    
    def calc_shortest_path_tree(self):
        self.shortest_path_map = defaultdict(lambda : None)
        
        for source_forwarding_element in self.forwarding_elements:
            src_node_id = source_forwarding_element.node_id
            nodes = set(self.forwarding_elements)
            edges = self.links
            graph = defaultdict(list)
            for link in edges:
                graph[link.tail_node_id].append((link.cost, link.head_node_id))
            
            src_path_tree_map = defaultdict(lambda : None)
            queue, seen = [(0,src_node_id,())], set()
            while queue:
                (cost,node1,path) = heappop(queue)
                if node1 not in seen:
                    seen.add(node1)
                    path = (node1, path)
                    src_path_tree_map[node1] = path
         
                    for next_cost, node2 in graph.get(node1, ()):
                        if node2 not in seen:
                            new_path_cost = cost + next_cost
                            heappush(queue, (new_path_cost, node2, path))
            
            for dst_forwarding_element in self.forwarding_elements:
                if self.shortest_path_map[src_node_id] is None:
                    self.shortest_path_map[src_node_id] = defaultdict(lambda : None)
                    
                dst_node_id = dst_forwarding_element.node_id
                shortest_path_edges = []
                
                if dst_node_id == src_node_id:
                    self.shortest_path_map[src_node_id][dst_node_id] = []
                    continue
                
                receiver_path = src_path_tree_map[dst_node_id]
                if receiver_path is None:
                    continue
                    
                while receiver_path[1]:
                    shortest_path_edges.append((receiver_path[1][0], receiver_path[0]))
                    receiver_path = receiver_path[1]
                
                self.shortest_path_map[src_node_id][dst_node_id] = shortest_path_edges
        
        self.recalc_path_tree_map = False
        
        # Recalculate the network diameter
        self.network_diameter = 0
        for source_forwarding_element in self.forwarding_elements:
            for dest_forwarding_element in self.forwarding_elements:
                path = self.get_shortest_path_tree(source_forwarding_element.node_id, [dest_forwarding_element.node_id])
                if path is not None and len(path) > self.network_diameter:
                    self.network_diameter = len(path)
        # print 'Got network diameter: ' + str(self.network_diameter)
        
    def get_shortest_path_tree(self, source_node_id, receiver_node_id_list):
        if self.recalc_path_tree_map:
            self.calc_shortest_path_tree()
        
        if len(receiver_node_id_list) == 1:
            return self.shortest_path_map[source_node_id][receiver_node_id_list[0]]
            
        shortest_path_tree_edges = Set()
        for receiver_node_id in receiver_node_id_list:
            shortest_path = self.shortest_path_map[source_node_id][receiver_node_id]
            if shortest_path is None:
                print 'ERROR: No shortest path from node ' + str(source_node_id) + ' to ' + str(receiver_node_id)
                return None
            for edge in shortest_path:
                shortest_path_tree_edges.add(edge)
        
        # Return the set as a list of edges
        shortest_path_tree_edges = list(shortest_path_tree_edges)
        return shortest_path_tree_edges
    
    def load_from_edge_list(self, edge_list):
        self.forwarding_elements = []
        self.links = []
        
        seen_node_ids = []
        for edge in edge_list:
            self.links.append(Link(edge[0], edge[1], 1))
            if edge[0] not in seen_node_ids:
                self.forwarding_elements.append(ForwardingElement(edge[0]))
                seen_node_ids.append(edge[0])
            
            if edge[1] not in seen_node_ids:
                self.forwarding_elements.append(ForwardingElement(edge[1]))
                seen_node_ids.append(edge[1])
        
        self.recalc_path_tree_map = True
                
    def load_from_brite_topo(self, brite_filepath, debug_print = False):
        self.forwarding_elements = []
        self.links = []
        
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
        
        # In the nodes section now, generate a forwarding element for each node
        while in_node_section:
            line = file.readline().strip()
            if not line:
                in_node_section = False
                if debug_print:
                    print 'Finished parsing nodes'
                break
            
            line_split = line.split('\t')
            node_id = int(line_split[0])
            if debug_print:
                print 'Generating forwarding element for ID: ' + str(node_id)
            self.forwarding_elements.append(ForwardingElement(node_id))
            
        # Skip ahead to the edges section
        in_edge_section = False
        while not in_edge_section:
            line = file.readline()
            if 'Edges:' in line:
                in_edge_section = True
                break
        
        # In the edges section now, add all required links
        # Note: This code assumes that all links are bidirectional with cost 1
        while in_edge_section:
            line = file.readline().strip()
            if not line:    # Empty string
                in_edge_section = False
                if debug_print:
                    print 'Finished parsing edges'
                break
                
            line_split = line.split('\t')
            node_id_1 = int(line_split[1])
            node_id_2 = int(line_split[2])
            if debug_print:
                print 'Adding bi-directional link between forwarding elements ' + str(node_id_1) + ' and ' + str(node_id_2)
            self.links.append(Link(node_id_1, node_id_2, 1))
            self.links.append(Link(node_id_2, node_id_1, 1))
        
        file.close()
        self.recalc_path_tree_map = True
        
    
    def __str__(self):
        return_str = '====================\nForwarding Elements:\n====================\n'
        for forwarding_element in self.forwarding_elements:
            return_str = return_str + str(forwarding_element) + '\n'
        return_str = return_str + '======\nLinks:\n======\n'
        for link in self.links:
            return_str = return_str + str(link) + '\n'
        return return_str

        
class McastGroup(object):
    def __init__(self, topology, src_node_id, bandwidth_Mbps, mcast_group_index):
        self.group_index = mcast_group_index
        self.src_node_id = src_node_id
        self.receiver_ids = Set()
        self.topology = topology
        self.bandwidth_Mbps = bandwidth_Mbps
        
        self.native_mcast_tree = None
        self.native_bandwidth_Mbps = None
        
        self.aggregated_mcast_tree_index = None
        self.aggregated_mcast_tree = None
        self.rendevouz_point_node_id = None
        self.rendevouz_point_shortest_path = None
        self.aggregated_bandwidth_Mbps = None
    
    def set_receiver_ids(self, receiver_ids):
        self.receiver_ids = Set(receiver_ids)
        self.native_mcast_tree = self.topology.get_shortest_path_tree(self.src_node_id, list(self.receiver_ids))
        self.native_bandwidth_Mbps = len(self.native_mcast_tree) * self.bandwidth_Mbps
        
    def generate_random_receiver_ids(self, num_receivers):
        # KLUDGE: Receiver IDs will always be generated until there is at least one receiver which is not colocated with the source
        # This prevents divide by 0 errors when calculating performance metrics
        # TODO - AC: Find a better way to handle this situation
        while len(self.receiver_ids) < num_receivers:
            new_node_id = randint(0, len(topo.forwarding_elements))
            if new_node_id != self.src_node_id and new_node_id not in self.receiver_ids:
                self.receiver_ids.add(new_node_id)
                
        self.native_mcast_tree = self.topology.get_shortest_path_tree(self.src_node_id, list(self.receiver_ids))
        self.native_bandwidth_Mbps = len(self.native_mcast_tree) * self.bandwidth_Mbps

    def jaccard_distance(self, mcast_group):
        return 1.0 - (float(len(self.receiver_ids.intersection(mcast_group.receiver_ids))) / float(len(self.receiver_ids.union(mcast_group.receiver_ids))))

    def debug_print(self):
        print 'Multicast Group #' + str(self.group_index) + '\nSrc Node ID: ' + str(self.src_node_id) + '\nReceivers: ',
        for receiver_id in self.receiver_ids:
            print str(receiver_id) + ', ',
        print ' '
        print 'Native Mcast Tree:\n' + str(self.native_mcast_tree)
        print 'Aggregated Mcast Tree Index: ' + str(self.aggregated_mcast_tree_index)
        print 'Aggregated Mcast Tree:\n' + str(self.aggregated_mcast_tree)
        print 'Rendevouz Point: Node #' + str(self.rendevouz_point_node_id) + '\nRendevouz Path: ' + str(self.rendevouz_point_shortest_path)
        
        
def run_multicast_aggregation_test(topo, num_groups, min_group_size, max_group_size, similarity_type, similarity_parameter, debug_print = False, plot_dendrogram = False):
    # Generate random multicast groups
    groups = []
    
    for i in range(0, num_groups):
        groups.append(McastGroup(topo, randint(0, len(topo.forwarding_elements)), 10, i))
        groups[i].generate_random_receiver_ids(randint(min_group_size, max_group_size + 1))
    
    #groups.append(McastGroup(topo, 0, 10, 0))
    #groups[0].set_receiver_ids([6,7])
    #groups.append(McastGroup(topo, 1, 10, 1))
    #groups[1].set_receiver_ids([6,7])
    #groups.append(McastGroup(topo, 8, 10, 2))
    #groups[2].set_receiver_ids([6,7])
    
    run_time_start = time()
    if 'single' in similarity_type or 'complete' in similarity_type or 'average' in similarity_type:
        groups, group_map = aggregate_groups_via_clustering(groups, similarity_type, similarity_parameter)
    elif 'tree_sim' in similarity_type:
        groups, group_map = aggregate_groups_via_tree_sim(topo, groups, similarity_parameter)
    else:
        print 'ERROR: Invalid similarity type - Supported options are "single", "average", "complete", or "tree_sim"'
        sys.exit(1)
    run_time = time() - run_time_start
    
    # Calculate network performance metrics
    bandwidth_overhead_ratio, flow_table_reduction_ratio, reducible_flow_table_reduction_ratio, num_trees = calc_network_performance_metrics(groups, group_map, True)
    
    return bandwidth_overhead_ratio, flow_table_reduction_ratio, reducible_flow_table_reduction_ratio, num_trees, run_time
    

if __name__ == '__main__':
    if len(sys.argv) < 5:
        print 'Tree aggregation script requires the following 5 command line arguments:'
        print '[1] Topology filepath (string)'
        print '[2] Number of trials to run (integer)'
        print '[3] Number of multicast groups (integer)'
        print '[4] Group size range (string, in format "1-10"). If only a single number is specified, the minimum group size is set to 1'
        print '[5] Similarity type (string): one of "single", "complete", "average", or "tree_sim"'
        print '[6] Similarity parameter (float):'
        print '\tFor the "single", "complete", and "average" similarity types this sets the similarity threshold to use for clustering'
        print '\tFor the "tree_sim" similarity type this sets the bandwidth overhead threshold'
        print 
        sys.exit(0)
    
    # Import the topology from BRITE format
    topo = SimTopo()
    topo.load_from_brite_topo(sys.argv[1])
    #print topo
    
    #topo.load_from_edge_list([[0,2],[1,2],[2,0],[2,1],[2,3],[3,2],[3,4],[4,3],[4,5],[5,6],[5,7], [8,0]])
    #similarity_threshold = 0.5
    #bandwidth_overhead_ratio, flow_table_reduction_ratio, num_clusters = run_multicast_aggregation_test(topo, similarity_threshold, 'single', True)
    #sys.exit(0)
    
    bandwidth_overhead_list = []
    flow_table_reduction_list = []
    reducible_flow_table_reduction_list = []
    num_trees_list = []
    run_time_list = []
    
    min_group_size = 1
    max_group_size = 10
    group_range_split = sys.argv[4].split('-')
    if len(group_range_split) == 1:
        max_group_size = int(group_range_split[0])
    else:
        min_group_size = int(group_range_split[0])
        max_group_size = int(group_range_split[1])
    
    num_trials = int(sys.argv[2])
    start_time = time()
    print 'Simulations started at: ' + str(datetime.now())
    
    for i in range(0, num_trials):
        #if i % 20 == 0:
        #    print 'Running trial #' + str(i)
        bandwidth_overhead_ratio, flow_table_reduction_ratio, reducible_flow_table_reduction_ratio, num_trees, run_time = \
                run_multicast_aggregation_test(topo, int(sys.argv[3]), min_group_size, max_group_size, sys.argv[5], float(sys.argv[6]), False, False)
                
        bandwidth_overhead_list.append(bandwidth_overhead_ratio)
        flow_table_reduction_list.append(flow_table_reduction_ratio)
        reducible_flow_table_reduction_list.append(reducible_flow_table_reduction_ratio)
        num_trees_list.append(num_trees)
        run_time_list.append(run_time)
        
    end_time = time()
    
    print ' '
    print 'Similarity Type: ' + sys.argv[5]
    print 'Similarity Threshold: ' + sys.argv[6]
    print 'Average Bandwidth Overhead: ' + str(sum(bandwidth_overhead_list) / len(bandwidth_overhead_list))
    print 'Average Flow Table Reduction: ' + str(sum(flow_table_reduction_list) / len(flow_table_reduction_list))
    print 'Average Reducible Flow Table Reduction: ' + str(sum(reducible_flow_table_reduction_list) / len(reducible_flow_table_reduction_list))
    print 'Average # Aggregated Trees: ' + str(float(sum(num_trees_list)) / len(num_trees_list))
    print 'Average Tree Agg. Run-Time: ' + str(float(sum(run_time_list)) / len(run_time_list))
    print 'Expected Sim Run-Time: ' + str((float(sum(run_time_list)) / len(run_time_list)) * num_trials)
    print ' '
        
    print 'Completed ' + str(num_trials) + ' trials in ' + str(end_time - start_time) + ' seconds (' + str(datetime.now()) + ')'
    sys.exit()