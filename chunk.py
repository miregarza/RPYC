# Chunk file 
#Referenced from:  https://github.com/sanketplus/PyDFS/blob/master/pydfs/minion.py
#
# This file will hold the data for all the files
#
# Sends the file directly to client after getting request
# from server

#####################################################
#                      Methods used
# read
# write
# delete
#####################################################
import rpyc
import os
import socket
from rpyc.utils.server import ThreadedServer


DATA_DIR = os.path.expanduser("~")#must define directory where files will be held
DATA_DIR += "/gfs_root/"

class ChunkService(rpyc.Service):
	class exposed_Chunks():
		#blocks = {} #to cover Google's consistency model

		def exposed_put(self, blockID, data, minions): #write
			with open(DATA_DIR+str(blockID), 'w') as f:
				f.write(data)
			if len(minions) > 0: #redo connection similar to server
				self.forward(blockID, data, minions)

		def exposed_get(self, blockID): #read
			fullfileloc=DATA_DIR+str(blockID)
			if not os.path.isfile(fullfileloc):
				return None
			with open(fullfileloc) as f:
				return f.read()

		def exposed_delete(self, blockID):
			fullfileloc = DATA_DIR + str(blockID)
			if not (os.path.isfile(fullfileloc)):
				return None
			os.remove(fullfileloc)
			return True

		def forward(self, blockID, data, minions):
			minion = minions[0]
			minions = minions[1:] #minions redefined until all covered
			host, port = minion
			con = rpyc.connect(host, port=port)
			minion = con.root.Chunks()
			minion.put(blockID,data,minions) #recursively calls self

if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)

ChunkThreads = ThreadedServer(ChunkService, port = 8888) #same port as minions in server
ChunkThreads.listener.settimeout(1)
ChunkThreads.start()