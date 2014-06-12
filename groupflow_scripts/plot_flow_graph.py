#!/usr/bin/env python
import sys
import pydot

def plot_network_util(flowtracker_log_filepath, topology_filepath = None):
    graph = pydot.Dot(graph_type='digraph', layout='dot', dim='2', overlap='scale', colorscheme='rdylgn11', splines='spline')
    
    if topology_filepath is not None:
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
        num_nodes = 0
        while in_node_section:
            line = topology_file.readline().strip()
            if not line:
                in_node_section = False
                break
            
            line_split = line.split('\t')
            node_id = int(line_split[0])
            num_nodes += 1
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
    num_edges = 0
    for line in flowtracker_log_file:
        if 'Final Network Topology:' in line:
            reached_net_summary = True
            continue
        
        if reached_net_summary:
            split_line = line.split(' ')
            switch1 = split_line[0]
            switch2 = split_line[3]
            
            # KLUDGE: Workaround to deal with Mininet assigning a random DPID to node 0
            if int(switch1) > 255:
                switch1 = '0'
            if int(switch2) > 255:
                switch2 = '0'
                
            util = float(split_line[5][len('U:'):])
            num_flows = int(split_line[6][len('NF:'):])
            label = str(num_flows) # + ' Util {:1.2f}'.format(util)
            color = None
            
            if util >= 1:
                color = '1'
            elif util > 0.90:
                color = '2'
            elif util > 0.80:
                color = '3'
            elif util > 0.70:
                color = '4'
            elif util > 0.60:
                color = '5'
            elif util > 0.50:
                color = '7'
            elif util > 0.40:
                color = '8'
            elif util > 0.30:
                color = '9'
            elif util > 0.20:
                color = '10'
            else:
                color = '11'
                
            print 'Adding Edge between Switch %s and Switch %s, Label: %s' % (switch1, switch2, label)
            num_edges += 1
            
            edge = None
            edge = pydot.Edge(switch1, switch2, dir="forward", arrowhead="normal", colorscheme='rdylgn11', color=color, fontcolor=color, style='setlinewidth(3)', label=str(label))
            
            # edge = pydot.Edge(switch1, switch2, dir="forward", arrowhead="normal", colorscheme='rdylgn11', color=color, fontcolor=color, style='setlinewidth(3)', label=str(label))
            #if util > 1:
            #    edge = pydot.Edge(switch1, switch2, dir="forward", arrowhead="normal", colorscheme='rdylgn11', color=color, fontcolor=color, style='setlinewidth(3)', label=str(label))
            #else:
            #    edge = pydot.Edge(switch1, switch2, dir="forward", arrowhead="normal", colorscheme='rdylgn11', color=color, fontcolor=color, style='setlinewidth(3)')
            
            graph.add_edge(edge)
    flowtracker_log_file.close()
    
    if topology_filepath is not None:
        print 'Processed network: %d Switches, %d Links' % (num_nodes, num_edges)
    else:
        print 'Processed network: %d Links' % (num_edges)
    
    graph.write_png(flowtracker_log_filepath + '.png')
    
if __name__ == '__main__':
    if len(sys.argv) >= 3:
        plot_network_util(sys.argv[1], sys.argv[2])
    if len(sys.argv) >= 2:
        plot_network_util(sys.argv[1])