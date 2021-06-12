# Server file
#Referenced from:  https://github.com/sanketplus/PyDFS/blob/master/pydfs/minion.py
#
# This file is in charge of recieving requests from the client
#
# Holds information about the file, its chunks, and where they
# are located
#
# Directs chunk servers to send info to 

#####################################################
#                      Methods used
# getfilelist
# read
# write
# exists: checks if filename exists
#####################################################

import rpyc
import socket
import uuid
import math
import random
import sys
from rpyc.utils.server import ThreadedServer

def chunkSetup():
	MasterService.exposed_Master.blocksize = 100 #helps determine how many blocks to allocate per file
	MasterService.exposed_Master.minions[1] = ("10.0.0.245", "8888") #port must match w forwarding port in chunks
	MasterService.exposed_Master.minions[2] = ("10.0.0.245", "9999")
	#minions should be id (index), ip, port


class MasterService(rpyc.Service):
	class exposed_Master():
		filelist = {} #to keep track of the block list
		minions = {}
		blocksize = 0

		def exposed_getFileList(self): #returns all files currently held
			return self.__class__.filelist.keys()

		def exposed_get(self, filename): #read
			if not(filename in self.__class__.filelist):
				return None
			return self.__class__.filelist[filename]

		def exposed_delete(self, filename):
			if not(filename in self.__class__.filelist):
				return False
			return (self.__class__.filelist.pop(filename, None))

		def exposed_put(self, dest, size): #write
			# if already exists w update do nothing
			if (dest in self.__class__.filelist):
				print("file already in list, skipping")
				pass
			#update otherwise
			self.__class__.filelist[dest] = []
			blockCount = int(math.ceil(float(size)/self.__class__.blocksize))
			blocks = self.alloc_blocks(dest, blockCount)
			return blocks


		def exposed_get_block_size(self):
			return self.__class__.blocksize

		def exposed_get_minions(self):
			return self.__class__.minions

		def alloc_blocks(self, dest, num):
			blocks = []
			for block in range(0, num):
				blockID = uuid.uuid1() #to give unique id to every block
				blockID = str(blockID)
				#nodesID = random.choice(self.__class__.minions.keys()) #randomly assigns a chunk server to allocate each block
				nodesID = random.sample(self.__class__.minions.keys(), 2) #2 stands for the replication, 2 servers chosen to hold val
				blocks.append((blockID, nodesID))
				#append blockID, chunkID, and index of block
				self.__class__.filelist[dest].append((blockID, nodesID, block))
			return blocks


# try:
# 	conn = rpyc.connect("localhost", 8100) #this is meant to work on an already opened port, not start conn
# 	print("Master is running")
# except:
# 	print("unable to connect")
chunkSetup()
serverThreads = ThreadedServer(MasterService, port = 10000) #creates new thread per connection
#note that MasterService is w/o (), meaning each connection will have its own instance of this service
serverThreads.start() #starts the server