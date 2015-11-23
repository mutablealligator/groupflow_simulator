#!/usr/bin/env python
import socket
import struct
import sys
import threading
import time
import binascii
import subprocess
import re

# To work in Mininet routes must be configured for hosts similar to the following:
# route add -net 224.0.0.0/4 h1-eth0

multicast_group = '224.1.1.1'
multicast_port = 5007
packets_to_receive = 0  # 0 specifies no limit to packet reception
echo_port = 5008

PACKET_SIZE = 4096
receive_packet_times = {}
receive_packet_index = 0

def getPath():
	return "/usr/local/home/cse222a05/GroupFlow/groupflow_scripts/output/"

def writeMessage(seq_num, filename, data, stime, dsttask):
	filename = getPath() + "message" + str(seq_num) 
	subprocess.call('touch ' + str(filename), shell=True)
	fileobj = open(filename, "w")
	matchObj = re.match(r'src:(\d+):mtag:(\d+):(.*)', data, re.M|re.I)
	fileobj.write(matchObj.group(1) + "\n")
	fileobj.write(matchObj.group(2) + "\n")
	dsttask = dsttask.lstrip('h')
	fileobj.write(dsttask + "\n")
	fileobj.write(str(len(matchObj.group(3))) + "\n")
	latency = time.time() - stime
	str_time = "{:0.6f}".format(latency * 1000)
	fileobj.write(str_time + "\n")
	fileobj.write(matchObj.group(3) + "\n")
	fileobj.write(data + '\n')
	fileobj.flush()
	fileobj.close()

def main():
    global multicast_group, multicast_port, packets_to_receive, echo_port, receive_packet_times, receive_packet_index
    
    if len(sys.argv) > 1:
        multicast_group = sys.argv[1]
    
    if len(sys.argv) > 2:
        multicast_port = int(sys.argv[2])
    
    if len(sys.argv) > 3:
        echo_port = int(sys.argv[3])

    if len(sys.argv) > 4:
	seq_num = int(sys.argv[4])
    else:
	seq_num = 0
        
    if len(sys.argv) > 5:
	stime = sys.argv[5]
    
    if len(sys.argv) > 6:
	dsttask = sys.argv[6]

    if len(sys.argv) > 7:
        packets_to_receive = int(sys.argv[7])
    
    # Setup the socket for receive multicast traffic
    multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    multicast_socket.bind(('', multicast_port))
    mreq = struct.pack("=4sl", socket.inet_aton(multicast_group), socket.INADDR_ANY)
    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    
    # Setup the socket for sending echo response
    echo_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    print "Receiving packets at : " + multicast_group + ":" + str(multicast_port)
    recv_packets = 0
    while True:
        try:
	    print "=" * 200
            data, addr = multicast_socket.recvfrom(PACKET_SIZE)
	    receive_packet_times[receive_packet_index] = time.time()	
	    str_time = '\t Time: ' + "{:0.6f}".format(receive_packet_times[receive_packet_index] * 1000) + ' ms'
	    print 'Received packet of data ' + str(data) + ' from sender ' + str(addr[0]) + ':' + str(echo_port) + ' at ' + str_time
            echo_socket.sendto(data, (addr[0], echo_port))
            print 'ACK Sending : Echo packet ' + str(data) + ' to ' + str(addr[0]) + ':' + str(echo_port) + ' at ' + str_time
	    if recv_packets == 0:
		writeMessage(seq_num, "message", data, stime, dsttask)
            recv_packets += 1
	    receive_packet_index += 1
        except socket.error, e:
            print 'Exception'
        
        if packets_to_receive != 0 and recv_packets > packets_to_receive:
            break
    
    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    
if __name__ == '__main__':
    main()
