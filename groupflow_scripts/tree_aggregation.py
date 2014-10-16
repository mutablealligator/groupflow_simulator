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

class BriteSimTopo(object):
    def __init__(self, brite_filepath, debug_print = False):
        self.forwarding_elements = []
        self.links = []
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
    
    def __str__(self):
        return_str = '====================\nForwarding Elements:\n====================\n'
        for forwarding_element in self.forwarding_elements:
            return_str = return_str + str(forwarding_element) + '\n'
        return_str = return_str + '======\nLinks:\n======\n'
        for link in self.links:
            return_str = return_str + str(link) + '\n'
        return return_str

def calc_shortest_path_tree(topology, source_node_id, receiver_node_id_list):
    nodes = set(topology.forwarding_elements)
    edges = topology.links
    graph = defaultdict(list)
    for link in edges:
        graph[link.tail_node_id].append((link.cost, link.head_node_id))
    
    #print graph
    
    path_tree_map = defaultdict(lambda : None)
    queue, seen = [(0,source_node_id,())], set()
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
    
    #print path_tree_map
    
    shortest_path_tree_edges = []
    calculated_path_node_ids = []
    for receiver_node_id in receiver_node_id_list:
        if receiver_node_id == source_node_id:
            continue
        if receiver_node_id in calculated_path_node_ids:
            continue
        
        receiver_path = path_tree_map[receiver_node_id]
        if receiver_path is None:
            log.warn('Path could not be determined for receiver ' + str(receiver_node_id) + ' (network is not fully connected)')
            continue
            
        while receiver_path[1]:
            shortest_path_tree_edges.append((receiver_path[1][0], receiver_path[0]))
            receiver_path = receiver_path[1]
        calculated_path_node_ids.append(receiver_node_id)
                
    # Get rid of duplicates in the edge list (must be a more efficient way to do this, find it eventually)
    shortest_path_tree_edges = list(Set(shortest_path_tree_edges))
    print shortest_path_tree_edges

class McastGroup(object):
    def __init__(self, mcast_group_index):
        self.group_index = mcast_group_index
        self.receiver_ids = Set()
        
    def generate_random_receiver_ids(self, num_receivers, max_receiver_id):
        # max_receiver_id is inclusive (i.e. receiver ids will be generated between 0 and max_receiver_id)
        while len(self.receiver_ids) < num_receivers:
            self.receiver_ids.add(randint(0, max_receiver_id + 1))

    def jaccard_distance(self, mcast_group):
        return 1.0 - (float(len(self.receiver_ids.intersection(mcast_group.receiver_ids))) / float(len(self.receiver_ids.union(mcast_group.receiver_ids))))

    def debug_print(self):
        print 'Multicast Group #' + str(self.group_index) + ' Receivers: '
        for receiver_id in self.receiver_ids:
            print str(receiver_id)
            
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
    
    print 'Group Aggregations for Difference Threshold: ' + str(difference_threshold)
    for cluster_index in group_map:
        print 'Cluster Index: ' + str(cluster_index)
        for group_index in group_map[cluster_index]:
            print str(group_index) + ' ',
        print ' '
            
    return 0

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        topo = BriteSimTopo(sys.argv[1])
        print topo
        calc_shortest_path_tree(topo, 5, [1,2,3,14])
        sys.exit(0)
        
    groups = []
    
    for i in range(0, 10):
        groups.append(McastGroup(i))
        groups[i].generate_random_receiver_ids(randint(1,10), 20)
        groups[i].debug_print()
        print ' ' 
    
    receivers_list = []
    for group in groups:
        receivers_list.append(list(group.receiver_ids))
    
    receivers_array = np.array(receivers_list)
    #print 'Receivers Array:\n' + str(receivers_array)
    distance_matrix = []
    group_index = 0
    group_indexes = []
    for group1 in groups:
        distance_matrix.append([])
        for group2 in groups:
            distance_matrix[group_index].append(group1.jaccard_distance(group2))
        group_indexes.append(group_index)
        group_index += 1
    #print 'Distance Array Len:' + str(len(distance_matrix))
    #print 'Distance Matrix:\n' + str(distance_matrix)
    print 'Total Num Groups: ' + str(len(group_indexes))
    
    comp_dist_array = ssd.squareform(distance_matrix)
    #print 'Compressed Distance Array Len: ' + str(len(comp_dist_array))
    
    plt.figure(1, figsize=(6, 5))
    z = linkage(comp_dist_array, method='single', metric='jaccard')
    print 'Linkage Array:\n' + str(z)
    print ' '
    get_group_aggregation(group_indexes, z, 0.7)
    d = dendrogram(z, show_leaf_counts=True)
    plt.title('Multicast Group Clustering (Single Linkage)')
    plt.xlabel('Multicast Group Index')
    plt.ylabel('Cluster Distance')
    
    plt.figure(2, figsize=(6, 5))
    z = linkage(comp_dist_array, method='complete', metric='jaccard')
    print 'Linkage Array:\n' + str(z)
    d = dendrogram(z, show_leaf_counts=True)
    plt.title('Multicast Group Clustering (Complete Linkage)')
    plt.xlabel('Multicast Group Index')
    plt.ylabel('Cluster Distance')
    plt.show()
    
    sys.exit()