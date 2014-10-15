from scipy.stats import truncnorm, tstd, poisson, expon
from numpy.random import randint, uniform
from datetime import datetime
from sets import Set
from time import time
from scipy.cluster.hierarchy import *
from scipy.spatial.distance import pdist
import scipy.spatial.distance as ssd
import os
import sys
import signal
import numpy as np
import matplotlib.pyplot as plt

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