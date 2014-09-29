from scipy.stats import truncnorm, tstd, poisson, expon
from numpy.random import randint, uniform
from datetime import datetime
from sets import Set
from time import time
from scipy.cluster.hierarchy import *
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

    def jaccard_index(self, mcast_group):
        return float(len(self.receiver_ids.intersection(mcast_group.receiver_ids))) / float(len(self.receiver_ids.union(mcast_group.receiver_ids)))

    def debug_print(self):
        print 'Multicast Group #' + str(self.group_index) + ' Receivers: '
        for receiver_id in self.receiver_ids:
            print str(receiver_id)

if __name__ == '__main__':
    groups = []
    
    for i in range(0, 20):
        groups.append(McastGroup(i))
        groups[i].generate_random_receiver_ids(5, 40)
        groups[i].debug_print()
        print ' ' 
    
    receivers_list = []
    for group in groups:
        receivers_list.append(list(group.receiver_ids))
    
    receivers_array = np.array(receivers_list)
    print 'Receivers Array:\n' + str(receivers_array)
    plt.figure(1, figsize=(6, 5))
    # a = np.array([[1, 4, 7, 9], [1, 3, 8, 10], [2, 3, 8, 11]]);
    z = linkage(receivers_array, method='single', metric='jaccard')
    print 'Linkage Array:\n' + str(z)
    d = dendrogram(z, show_leaf_counts=True)
    plt.title('Multicast Group Clustering')
    plt.xlabel('Multicast Group Index')
    plt.ylabel('Cluster Distance')
    plt.show()
    
    sys.exit()