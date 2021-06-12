# client file
# 
#Referenced from:  https://github.com/sanketplus/PyDFS/blob/master/pydfs/minion.py
# This file is in charge of interacting with the user to communicate
# about file upload/retrieval
#
# sends file req to server
#
# recieves file from chunk servers
#
# user will choose actions by entering:
#    filename.py action file

# #####################################################
#                     Methods used
# put: upload file
# list: list files in system
# get: retrieve file
# #####################################################

import rpyc
import sys
import os
import time

startingTime = time.time()

def put(master, filesrc, filedest):
	print("upload/overwrite")
	# determine filesize
	# split into blocks
	blocks = master.put(filedest, os.path.getsize(filesrc)) #add to file list
	# save blocks
	with open(filesrc) as f: #go through each block and allocate w chunk(minion)
		for block in blocks:
			data = f.read(master.get_block_size())
			blockID = block[0]
			minions = [master.get_minions()[_] for _ in block[1]]
			# for i in list(block[1]):
			# 	minions[i] = master.get_minions()[i]
			send_to_minion(blockID, data, minions) #allocating w chunk server, connection done here
	print("file succesfully saved in chunk servers")


def delete_file_chunks(blockID, minion):
	host, port = minion
	try:
		con = rpyc.connect(host, port=port)
		minion = con.root.Chunks()
	except:
		print "\n----Chunk Server not found -------"
		print "----Start Chunks.py then try again ------ \n \n "
		sys.exit(1)
	return minion.delete(blockID)


def get(master, filedest):
	print("download")
	filelist = master.get(filedest) #lookup file in master
	if not filelist:# lookup file in master
		print("file is not in the list. No changes made")
		return
	# if available
	for block in filelist:#go through each block
		for m in [master.get_minions()[_] for _ in block[1]]:
			data = read_from_minion(block[0], m)
			if data:
				sys.stdout.write(data)# return file portion
				break
		else:
			print "Err: Block file missed "
	

def delete(master, filename):
	print("delete")
	filelist = master.delete(filename)
	if not filelist:# lookup file in master
		print("file is not in the list. No changes made")
		return
	# if available
	print("file removed from master list")
	
	for block in filelist: #for each block
		for minion in [master.get_minions()[_] for _ in block[1]]:
			if not(delete_file_chunks(block[0], minion)):#delete block
				print ("file not found in chunks")
		print("file removed from chunk server")

def listFiles(master):
	print("Files available: ")
	print(master.getFileList())
	# retrieve filelist in master
	# print files

def send_to_minion(blockID, data, minions):
	minion = minions[0]
	minions = minions[1:]
	host, port = minion
	try:
		print("ATTEMPTING TO CONNECT TO CHUNK SERVER")
		# print("host: ", host, " port: ", port)
		conn = rpyc.connect(host, port=port) #connecting to chunk
		minion = conn.root.Chunks()
		minion.put(blockID, data, minions) #goes through chunk method
	except:
		print ("\n----Chunk Server not found -------")
		print ("----Start Chunk.py then try again ------ \n \n ")
		sys.exit(1)

def read_from_minion(blockID, minion):
	host, port = minion
	try:
		con = rpyc.connect(host, port=port)
		minion = con.root.Chunks()
	except:
		print "\n----Chunk Server not found -------"
		print "----Start Chunks.py then try again ------ \n \n "
		sys.exit(1)
	return minion.get(blockID)


try:
	conn = rpyc.connect("10.0.0.130", port = 10000) #connecting to master
	master = conn.root.Master() #gives connection access to service that is exposed by other party
	#in this case, allows to access master's exposed attributes/methods from class exposed_Master()
	print("Connected to master")
except:
	print("Master server issue")


userinput = sys.argv[1:] #userinput[i] i= (0=action, 1=filename/loc, 2=filename)

if len(userinput) == 0:
	print("Must run client file with command")
	print("--------------------- Actions available below ---------------------")
	print("Upload/Overwrite ... client.py put src/file/location GFS_filename")
	print("Download ........... client.py get GFS_filename")
	print("Delete ............. client.py del GFS_filename")
	print("List Files ......... client.py list")
	print("-------------------------------------------------------------------")
else:
	userAction = userinput[0]
	if(userAction == "put"): #upload/overwrite, need: fileloc filename
		put(master, userinput[1], userinput[2])
	elif(userAction == "get"): #download, need: filename
		get(master, userinput[1])
	elif(userAction == "del"): #delete, need: filename
		delete(master, userinput[1])
	elif(userAction == "list"): #list items in GFS
		listFiles(master)
endingTime = time.time()
print("Time elapsed: ", (endingTime - startingTime))
