import sys
import os
import time
from time import sleep

hashMap = []

class Message(object):
	def __init__(self, msg, length, mtag, sender, receivers = []):
		self.msg = msg
		self.length = length
		self.mtag = mtag
		self.sender = sender
		self.receivers = receivers
	
	def debug_print(self):
		print "Sending Message of length " + str(self.length) + " from " + self.sender + " to " + ",".join(self.receivers)
		print "Message is : " + self.getMessage()
	
	def getSender(self):
		return self.sender

	def getReceivers(self):
		return self.receivers
	
	def getMessage(self):
		return self.msg

	def getPacketSize(self):
		return self.length

	def getNofReceivers(self):
		return len(self.receivers)

def parseFile(filename):
	text_file = open(filename, "r")
	lines = text_file.readlines()
	text_file.close()
	for i in range(0,len(lines)):
		lines[i] = lines[i].strip('\r\n')
	sender = 'h' + lines[0]
	mtag = lines[1]
	nr = lines[2]
	nr = int(nr)
	recvs = lines[3:3+nr]
	for i in range(0, len(recvs)):
		recvs[i] = "h" + recvs[i]
	length = lines[3+nr]
	content = lines[3+nr+1:len(lines)]
	msg = ""
	for m in content:
		msg += m
	return Message(msg, length, mtag, sender, recvs)

def getPath():
	return "/usr/local/home/cse222a05/GroupFlow/groupflow_scripts/input/"

def watchForFiles():
	global hashMap
	path = getPath()
	while True:
		print 'Waiting for new message...'
		filelist = [os.path.join(path, fn) for fn in next(os.walk(path))[2]]
		filelist = filter(lambda x: not os.path.isdir(x), filelist)
		newest = max(filelist, key=lambda x: os.stat(x).st_mtime)
		if newest not in hashMap:
			return newest
		for f in filelist:
			if f not in hashMap:
				return f
		sleep(1)

def initHashMap():
	fileobj = open("/usr/local/home/cse222a05/GroupFlow/groupflow_scripts/msglist", "r")
	msgs = fileobj.readlines();
	for i in range(0,len(msgs)):
		msgs[i] = msgs[i].strip('\r\n')
		hashMap.append(msgs[i])
	fileobj.close()

def writeHashMap(filename):
	fileobj = open("/usr/local/home/cse222a05/GroupFlow/groupflow_scripts/msglist", "a")
	fileobj.write(filename + "\n")
	fileobj.flush()
	fileobj.close()

def getFiles():
	path = "/usr/local/home/cse222a05/GroupFlow/groupflow_scripts/input/"
	paths = [os.path.join(path,fn) for fn in next(os.walk(path))[2]]
	print paths

def recvMsg():
	global hashMap
	initHashMap()
	print 'Waiting for message from MPI Application...'
	filename = watchForFiles()
	print filename
	print 'Got a message. Preparing to send '
	message = parseFile(filename)
	message.debug_print()
	writeHashMap(filename)
	return message
