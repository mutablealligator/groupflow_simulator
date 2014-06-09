#!/usr/bin/env python
import sys
import pydot

def plot_network_util(flowtracker_log_filepath, topology_filepath):
    graph = pydot.Dot(graph_type='digraph', layout='neato', dim='2', overlap='prism', colorscheme='rdylgn11')
    
    # Generate nodes
    topology_file = open(topology_filepath, 'r')
    
    # Skip ahead until the nodes section is reached
    in_node_section = False
    while not in_node_section:
        line = topology_file.readline()
        if 'Nodes:' in line:
            in_node_section = True
            break
    
    # In the nodes section now, generate a switch and host for each node
    while in_node_section:
        line = topology_file.readline().strip()
        if not line:
            in_node_section = False
            break
        
        line_split = line.split('\t')
        node_id = int(line_split[0])
        node_x_pos = float(line_split[1]) / 72
        node_y_pos = float(line_split[2]) / 72
        position = '%f,%f!' % (node_x_pos, node_y_pos)
        print 'Adding Switch %d with Position: %s' % (node_id, position)
        node = pydot.Node(str(node_id), pos=position)
        graph.add_node(node)
    topology_file.close()
    
    # Generate edges
    flowtracker_log_file = open(flowtracker_log_filepath, 'r')
    reached_net_summary = False
    for line in flowtracker_log_file:
        if 'Final Network Topology:' in line:
            reached_net_summary = True
            continue
        
        if reached_net_summary:
            split_line = line.split(' ')
            switch1 = split_line[0]
            if '7004852015439' in switch1:
                switch1 = '0'
            
            switch2 = split_line[3]
            if '7004852015439' in switch2:
                switch2 = '0'
                
            util = float(split_line[5][len('U:'):])
            util_string = '{:1.2f}'.format(util)
            color = None
            if util > 1:
                color = '1'
            elif util > 0.9:
                color = '2'
            elif util > 0.8:
                color = '3'
            elif util > 0.7:
                color = '4'
            elif util > 0.6:
                color = '5'
            elif util > 0.5:
                color = '6'
            elif util > 0.4:
                color = '7'
            elif util > 0.3:
                color = '8'
            elif util > 0.2:
                color = '9'
            elif util > 0.1:
                color = '10'
            else:
                color = '11';
            print 'Adding Edge between Switch %s and Switch %s' % (switch1, switch2)
            edge = pydot.Edge(switch1, switch2, dir="forward", arrowhead="normal", colorscheme='rdylgn11', color=color, fontcolor=color, style='setlinewidth(2)')
            graph.add_edge(edge)
    flowtracker_log_file.close()
    
    graph.write_png(flowtracker_log_filepath + '.png')
    
if __name__ == '__main__':
    if len(sys.argv) >= 3:
        plot_network_util(sys.argv[1], sys.argv[2])