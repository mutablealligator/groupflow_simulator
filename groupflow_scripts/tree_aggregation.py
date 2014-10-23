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

def get_group_aggregation(group_indexes, linkage_array, difference_threshold):
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

def generate_aggregated_mcast_trees(topology, mcast_groups, group_map):
    for group_aggregation in group_map:
        # print 'Cluster #' + str(group_aggregation) + ' - Groups: ' + (str(group_map[group_aggregation]))
        cluster_receivers = []
        for mcast_group_id in group_map[group_aggregation]:
            mcast_groups[mcast_group_id].aggregated_mcast_tree_index = group_aggregation
            for receiver_id in mcast_groups[mcast_group_id].receiver_ids:
                cluster_receivers.append(receiver_id)
        
        cluster_receivers = list(Set(cluster_receivers))
        
        # First - determine rendevouz point for the group aggregation
        min_sum_path_length = sys.maxint
        rv_node_id = None
        for forwarding_element in topology.forwarding_elements:
            no_rendevouz_path = False
            sum_path_length = 0
            potential_rv_node_id = forwarding_element.node_id
            for mcast_group_id in group_map[group_aggregation]:
                src_node_id = mcast_groups[mcast_group_id].src_node_id
                # print 'SrcNode: ' + str(src_node_id) + ' RVNode: ' + str(potential_rv_node_id)
                shortest_path = topology.get_shortest_path_tree(src_node_id, [potential_rv_node_id])
                # print shortest_path
                if shortest_path is None:
                    no_rendevouz_path = True
                    break
                sum_path_length = sum_path_length + len(shortest_path)
            
            # print 'RVNode: ' + str(potential_rv_node_id) + ' PathLength: ' + str(sum_path_length)
            if no_rendevouz_path:
                continue
                
            if sum_path_length <= min_sum_path_length:
                # Optimization - If multiple forwarding elements have the same sum rendevouz path length, choose
                # the rendevouz point that results in the shortest distribution tree (in number of hops)
                if sum_path_length == min_sum_path_length:
                    # print 'Found rendevouz points (' + str(potential_rv_node_id) + ',' + str(rv_node_id) + ') with equal path length (' + str(sum_path_length) + ')'
                    aggregated_mcast_tree_new = topology.get_shortest_path_tree(potential_rv_node_id, cluster_receivers)
                    aggregated_mcast_tree_old = topology.get_shortest_path_tree(rv_node_id, cluster_receivers)
                    # print 'len(new_tree): ' + str(len(aggregated_mcast_tree_new)) + ' len(old_tree): ' + str(len(aggregated_mcast_tree_old))
                    if len(aggregated_mcast_tree_new) > len(aggregated_mcast_tree_old):
                        # print 'Chose old rendevouz point'
                        continue
                    # print 'Chose new rendevouz point'
                    
                min_sum_path_length = sum_path_length
                rv_node_id = potential_rv_node_id
                for mcast_group_id in group_map[group_aggregation]:
                    src_node_id = mcast_groups[mcast_group_id].src_node_id
                    shortest_path = topology.get_shortest_path_tree(src_node_id, [potential_rv_node_id])
                    mcast_groups[mcast_group_id].rendevouz_point_node_id = rv_node_id
                    mcast_groups[mcast_group_id].rendevouz_point_shortest_path = shortest_path
                    mcast_groups[mcast_group_id].aggregated_mcast_tree = topology.get_shortest_path_tree(rv_node_id, cluster_receivers)
                    # print 'Set new aggregated mcast tree with len: ' + str(len(mcast_groups[mcast_group_id].aggregated_mcast_tree))
                    mcast_groups[mcast_group_id].aggregated_bandwidth_Mbps = (len(mcast_groups[mcast_group_id].aggregated_mcast_tree) + len(mcast_groups[mcast_group_id].rendevouz_point_shortest_path)) * mcast_groups[mcast_group_id].bandwidth_Mbps
        
        # print 'Cluster rendevouz point at forwarding element #' + str(rv_node_id)

def get_terminal_vertices(edge_list):
    tail_set = Set()
    head_set = Set()
    for edge in edge_list:
        tail_set.add(edge[0])
        head_set.add(edge[1])
    
    return head_set - tail_set

def get_non_terminal_vertices(edge_list):
    tail_set = Set()
    head_set = Set()
    for edge in edge_list:
        tail_set.add(edge[0])
        head_set.add(edge[1])
    
    return tail_set.intersection(head_set).union(tail_set - head_set)

    
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
        self.path_tree_map = defaultdict(lambda : None)
        self.recalc_path_tree_map = True
    
    def calc_shortest_path_tree(self):
        for source_forwarding_element in self.forwarding_elements:
            nodes = set(self.forwarding_elements)
            edges = self.links
            graph = defaultdict(list)
            for link in edges:
                graph[link.tail_node_id].append((link.cost, link.head_node_id))
            
            path_tree_map = defaultdict(lambda : None)
            queue, seen = [(0,source_forwarding_element.node_id,())], set()
            while queue:
                (cost,node1,path) = heappop(queue)
                if node1 not in seen:
                    seen.add(node1)
                    path = (node1, path)
                    path_tree_map[node1] = path
         
                    for next_cost, node2 in graph.get(node1, ()):
                        if node2 not in seen:
                            new_path_cost = cost + next_cost
                            heappush(queue, (new_path_cost, node2, path))
            
            self.path_tree_map[source_forwarding_element.node_id] = path_tree_map
        
        self.recalc_path_tree_map = False
        
    def get_shortest_path_tree(self, source_node_id, receiver_node_id_list):
        if self.recalc_path_tree_map:
            self.calc_shortest_path_tree()
            
        shortest_path_tree_edges = []
        calculated_path_node_ids = []
        for receiver_node_id in receiver_node_id_list:
            if receiver_node_id == source_node_id:
                continue
            if receiver_node_id in calculated_path_node_ids:
                continue
            
            receiver_path = self.path_tree_map[source_node_id][receiver_node_id]
            if receiver_path is None:
                print 'Path could not be determined for receiver ' + str(receiver_node_id) + ' (network is not fully connected)'
                return None
                
            while receiver_path[1]:
                shortest_path_tree_edges.append((receiver_path[1][0], receiver_path[0]))
                receiver_path = receiver_path[1]
            calculated_path_node_ids.append(receiver_node_id)
                    
        # Get rid of duplicates in the edge list (must be a more efficient way to do this, find it eventually)
        shortest_path_tree_edges = list(Set(shortest_path_tree_edges))
        # print shortest_path_tree_edges
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
        while len(self.receiver_ids) < num_receivers:
            self.receiver_ids.add(randint(0, len(topo.forwarding_elements)))
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
        
def run_multicast_aggregation_test(topo, similarity_threshold, debug_print = False, plot_dendrogram = False):
    # Generate random multicast groups
    groups = []
    
    for i in range(0, 10):
        groups.append(McastGroup(topo, randint(0, len(topo.forwarding_elements)), 10, i))
        groups[i].generate_random_receiver_ids(randint(1,10))
    
    #groups.append(McastGroup(topo, 0, 10, 0))
    #groups[0].set_receiver_ids([6,7])
    #groups.append(McastGroup(topo, 1, 10, 1))
    #groups[1].set_receiver_ids([6,7])
    
    
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
            distance_matrix[group_index].append(group1.jaccard_distance(group2))
        group_indexes.append(group_index)
        group_index += 1
    #print 'Total Num Groups: ' + str(len(group_indexes))
    comp_dist_array = ssd.squareform(distance_matrix)
    
    # Perform clustering, and plot a dendrogram of the results
    z = linkage(comp_dist_array, method='single', metric='jaccard')
    group_map = get_group_aggregation(group_indexes, z, similarity_threshold)
    
    if plot_dendrogram:
        plt.figure(1, figsize=(6, 5))
        print 'Linkage Array:\n' + str(z)
        print ' '
        d = dendrogram(z, show_leaf_counts=True)
        plt.title('Multicast Group Clustering (Single Linkage)')
        plt.xlabel('Multicast Group Index')
        plt.ylabel('Cluster Distance')
        plt.show()
    
    # Generate aggregated multicast trees based on the generated clusters
    generate_aggregated_mcast_trees(topo, groups, group_map)
    
    # Calculate group aggregation metrics
    native_network_flow_table_size = 0
    aggregated_network_flow_table_size = 0
    
    native_bandwidth_Mbps = 0
    aggregated_bandwidth_Mbps = 0
    
    seen_aggregated_tree_indexes = []
    for cluster_index in group_map:
        if len(group_map[cluster_index]) == 1:
            seen_aggregated_tree_indexes.append(cluster_index)
    
    for group in groups:
        native_bandwidth_Mbps = native_bandwidth_Mbps + group.native_bandwidth_Mbps
        aggregated_bandwidth_Mbps = aggregated_bandwidth_Mbps + group.aggregated_bandwidth_Mbps
        
        native_network_flow_table_size = native_network_flow_table_size + len(group.native_mcast_tree) + 1
        if len(group_map[group.aggregated_mcast_tree_index]) == 1:
            # Group does not use an aggregated tree
            aggregated_network_flow_table_size = aggregated_network_flow_table_size + len(group.native_mcast_tree) + 1
        else:
            # Group does use aggregated tree
            aggregated_network_flow_table_size = aggregated_network_flow_table_size + (len(group.receiver_ids) + 1) + len(group.rendevouz_point_shortest_path)
        
        if group.aggregated_mcast_tree_index not in seen_aggregated_tree_indexes:
            seen_aggregated_tree_indexes.append(group.aggregated_mcast_tree_index)
            aggregated_network_flow_table_size = aggregated_network_flow_table_size + (len(group.aggregated_mcast_tree) - len(get_terminal_vertices(group.aggregated_mcast_tree)))
        
        if debug_print:
            print ' '
            group.debug_print()
    
    bandwidth_overhead_ratio = float(aggregated_bandwidth_Mbps) / float(native_bandwidth_Mbps)
    flow_table_reduction_ratio = float(aggregated_network_flow_table_size) / float(native_network_flow_table_size)
    if debug_print:
        print ' '
        print 'Aggregated Network Bandwidth Utilization: ' + str(aggregated_bandwidth_Mbps) + ' Mbps'
        print 'Native Network Bandwidth Utilization: ' + str(native_bandwidth_Mbps) + ' Mbps'
        print 'Bandwidth Overhead Ratio: ' + str(bandwidth_overhead_ratio)
        print ' '
        print 'Native Network Flow Table Size: ' + str(native_network_flow_table_size)
        print 'Aggregated Network Flow Table Size: ' + str(aggregated_network_flow_table_size)
        print 'Flow Table Reduction Ratio: ' + str(flow_table_reduction_ratio)
    
    #plt.figure(2, figsize=(6, 5))
    #z = linkage(comp_dist_array, method='complete', metric='jaccard')
    #print 'Linkage Array:\n' + str(z)
    #d = dendrogram(z, show_leaf_counts=True)
    #plt.title('Multicast Group Clustering (Complete Linkage)')
    #plt.xlabel('Multicast Group Index')
    #plt.ylabel('Cluster Distance')
    
    return bandwidth_overhead_ratio, flow_table_reduction_ratio
    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Topology filepath was not specified.'
        sys.exit(1)
    
    # Import the topology from BRITE format
    topo = SimTopo()
    topo.load_from_brite_topo(sys.argv[1])
    print topo
    
    #topo.load_from_edge_list([[0,2],[1,2],[2,0],[2,1],[2,3],[3,4],[4,5],[5,6],[5,7]])
    #similarity_threshold = 0.5
    #bandwidth_overhead_ratio, flow_table_reduction_ratio = run_multicast_aggregation_test(topo, similarity_threshold, True)
    #sys.exit(0)
    
    bandwidth_overhead_list = []
    flow_table_reduction_list = []
    
    num_trials = 5000
    similarity_threshold = 0.5
    start_time = time()
    print 'Simulations started at: ' + str(datetime.now())
    for i in range(0, num_trials):
        if i % 100 == 0:
            print 'Running trial #' + str(i)
        bandwidth_overhead_ratio, flow_table_reduction_ratio = run_multicast_aggregation_test(topo, similarity_threshold)
        bandwidth_overhead_list.append(bandwidth_overhead_ratio)
        flow_table_reduction_list.append(flow_table_reduction_ratio)
    end_time = time()
    
    print ' '
    print 'Completed ' + str(num_trials) + ' trials in ' + str(end_time - start_time) + ' seconds (' + str(datetime.now()) + ')'
    print 'Average Bandwidth Overhead: ' + str(sum(bandwidth_overhead_list) / len(bandwidth_overhead_list))
    print 'Average Flow Table Reduction: ' + str(sum(flow_table_reduction_list) / len(flow_table_reduction_list))
    
    sys.exit()