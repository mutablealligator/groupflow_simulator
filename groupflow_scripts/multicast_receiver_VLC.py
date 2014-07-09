#!/usr/bin/env python
import socket
import struct
import sys
import threading
import time
import binascii
import signal
import os

# To work in Mininet routes must be configured for hosts similar to the following:
# route add -net 224.0.0.0/4 h1-eth0

# Command to start VLC streaming in a Mininet host:
# > su -c 'vlc test_file.mp4 -I dummy --sout "#rtp{access=udp, mux=ts, proto=udp, dst=224.1.1.1, port=5007}"' nonroot
# where 'nonroot' can be replaced by any valid non-root username

multicast_group = '224.1.1.1'
multicast_port = 5007
packets_to_receive = 0
echo_port = 5008
MPEG2_SECONDS_PER_TICK = 1.0 / (90.0 * 1000.0)  # MPEG2 uses 32 bit 90K Hz timestamps
SEQ_NUM_ROLLOVER = 65535
SEQ_OUT_OF_ORDER_THRESHOLD = 200

recv_packets = 0
recv_bytes = 0
lost_packets = 0

log_filename = None

def int_to_ip(ip):
    return socket.inet_ntoa(hex(ip)[2:].zfill(8).decode('hex'))

def print_packet_stats(sig = None, frame = None):
    if log_filename is not None:
        log_file = open(log_filename, 'w')
        log_file.write('RecvPackets:' + str(recv_packets) + ' RecvBytes:' + str(recv_bytes) + ' LostPackets:' 
                + str(lost_packets) + '\n')
        log_file.flush()
        log_file.close()
        
    if signal is not None:
        # Remove this signal handler, and rethrow the signal
        signal.signal(sig, signal.SIG_DFL)
        os.kill(os.getpid(), sig)


def main():
    global multicast_group, multicast_port, packets_to_receive, echo_port, log_filename, recv_packets, recv_bytes, lost_packets
    signal.signal(signal.SIGINT, print_packet_stats)
    signal.signal(signal.SIGTERM, print_packet_stats)
    
    if len(sys.argv) > 1:
        multicast_group = sys.argv[1]
    
    if len(sys.argv) > 2:
        multicast_port = int(sys.argv[2])
    
    if len(sys.argv) > 3:
        echo_port = int(sys.argv[3])
        
    if len(sys.argv) > 4:
        log_filename = sys.argv[4]
    
    # Setup the socket for receive multicast traffic
    multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    multicast_socket.bind(('', multicast_port))
    mreq = struct.pack("=4sl", socket.inet_aton(multicast_group), socket.INADDR_ANY)
    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    
    print 'RTP streaming client listening on address: ' + str(multicast_group) + ':' + str(multicast_port)
    print 'Running until ' + str(packets_to_receive) + ' packets have been received'
    
    first_packet = True
    last_sequence_num = 0
    
    new_packet_arrival_time = 0
    last_packet_arrival_time = 0
    new_packet_rtp_time = 0
    last_packet_rtp_time = 0
    delivery_delay = 0
    last_delivery_delay = 0
    jitter = 0
    
    try:
        while True:
            data, addr = multicast_socket.recvfrom(8192)    # Arbitrary maximum size
            new_packet_arrival_time = time.time()
            if last_packet_arrival_time == 0:
                print 'Received first RTP packet.'
            #else:
            #    print '\nReceived RTP packet with interarrival time: ' + '{:10.8f}'.format((new_packet_arrival_time - last_packet_arrival_time) * 1000) + ' ms'
                
            # Note: data should contain an RTP payload at this point
            # Read the RTP header
            sequence_num, timestamp = struct.unpack('!HI', data[2:8])
            new_packet_rtp_time = float(timestamp * MPEG2_SECONDS_PER_TICK)
            # print 'Sequence Num: ' + str(sequence_num)
            # print 'Timestamp: ' + str(timestamp)
            
            # Calculate jitter if this is not the first packet
            if not last_packet_rtp_time == 0:
                packet_arrival_interval = (new_packet_arrival_time - last_packet_arrival_time)
                rtp_time_interval = (new_packet_rtp_time - last_packet_rtp_time)
                #print 'Interarrival Time: ' + '{:10.8f}'.format(packet_arrival_interval * 1000) + ' ms   RTP time interval: ' \
                #        + '{:10.8f}'.format(rtp_time_interval * 1000) + ' ms'
                delivery_delay = packet_arrival_interval - rtp_time_interval
                #print 'Delivery delay: ' + '{:10.8f}'.format(delivery_delay * 1000) + ' ms'
                jitter = jitter + ((abs(delivery_delay) - jitter) / 16)
                #print 'Jitter estimate: ' + '{:10.8f}'.format(jitter * 1000) + ' ms'
                
            # Setup parameters for next packet read
            last_packet_arrival_time = new_packet_arrival_time
            last_packet_rtp_time = new_packet_rtp_time
            last_delivery_delay = delivery_delay
            
            # Update packet / byte counters
            recv_bytes += len(data)
            recv_packets += 1
            if first_packet:
                first_packet = False
            else:
                packet_interval = (sequence_num - last_sequence_num) - 1;
                # KLUDGE: If the current sequence number is only slightly less than the previous sequence number, assume that
                # this is an out of order packets arriving, and not a sequence number rollover
                if packet_interval < 0 and packet_interval >= -SEQ_OUT_OF_ORDER_THRESHOLD:
                    packet_interval = 0
                    if lost_packets > 0:
                        lost_packets -= 1
                elif packet_interval < 0:
                    packet_interval = ((sequence_num - last_sequence_num) + SEQUENCE_NUM_ROLLOVER)
                lost_packets += packet_interval   
            last_sequence_num = sequence_num
        
            if packets_to_receive > 0 and recv_packets > packets_to_receive:
                break
    
    except BaseException as e:
            print 'Exception: ' + str(e)
            print_packet_stats()
    
    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    print_packet_stats()
    
if __name__ == '__main__':
    main()