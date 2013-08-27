#!/usr/bin/env python
import socket
import struct
import sys
import threading
import time
import binascii

# To work in Mininet routes must be configured for hosts similar to the following:
# route add -net 224.0.0.0/4 h1-eth0

multicast_group = '224.1.1.1'
source_address = '10.0.0.1'
multicast_port = 5007
packets_to_receive = 30
echo_port = 5008

# WARNING: These attributes are very unlikely to work across all platforms... best that can be
# done until the python socket module is updated
if not hasattr(socket, 'IP_UNBLOCK_SOURCE'):
    setattr(socket, 'IP_UNBLOCK_SOURCE', 37)
if not hasattr(socket, 'IP_BLOCK_SOURCE'):
    setattr(socket, 'IP_BLOCK_SOURCE', 38)
if not hasattr(socket, 'IP_ADD_SOURCE_MEMBERSHIP'):
    setattr(socket, 'IP_ADD_SOURCE_MEMBERSHIP', 39)
if not hasattr(socket, 'IP_DROP_SOURCE_MEMBERSHIP'):
    setattr(socket, 'IP_DROP_SOURCE_MEMBERSHIP', 40)
    
if not hasattr(socket, 'MCAST_EXCLUDE'):
    setattr(socket, 'MCAST_EXCLUDE', 0)
if not hasattr(socket, 'MCAST_INCLUDE'):
    setattr(socket, 'MCAST_INCLUDE', 1)

def main():
    global multicast_group, multicast_port, packets_to_receive, source_address
    
    if len(sys.argv) > 1:
        multicast_group = sys.argv[1]
    
    if len(sys.argv) > 2:
        multicast_port = sys.argv[2]
        
    if len(sys.argv) > 3:
        packets_to_receive = int(sys.argv[3])
    
    if len(sys.argv) > 4:
        source_address = sys.argv[4]
    
    # Setup the socket for receive multicast traffic
    multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # print 'multicast_group: ' + str(multicast_group)
    # print 'source_address: ' + str(source_address)
    mreq = struct.pack("=4sl4s", socket.inet_aton(multicast_group), socket.INADDR_ANY, socket.inet_aton(source_address))
    multicast_socket.setsockopt(socket.SOL_IP, socket.IP_ADD_SOURCE_MEMBERSHIP, mreq)
    multicast_socket.bind((multicast_group, multicast_port))
    
    # Setup the socket for sending echo response
    echo_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    recv_packets = 0
    while True:
        try:
            data, addr = multicast_socket.recvfrom(128)
            echo_socket.sendto(data, (addr[0], echo_port))
            print 'Echo packet ' + str(int(data)) + ' to ' + str(addr[0]) + ':' + str(echo_port)
            recv_packets += 1
        except socket.error, e:
            print 'Exception'
        
        if recv_packets > packets_to_receive:
            break
    
    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_SOURCE_MEMBERSHIP, mreq)
    # mreq = struct.pack("=4sl", socket.inet_aton(multicast_group), socket.INADDR_ANY)
    # multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    
if __name__ == '__main__':
    main()