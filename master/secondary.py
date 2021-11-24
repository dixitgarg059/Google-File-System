import math
import socket
import pickle
import operator
import threading

HOST = "127.0.0.1"
PRIMARY_PORT = 8080
SECONDARY_PORT = 8081

MAXSIZE = 2048

chunkservers = {}
files = {}

primaryStatus = True

def getFileName(name):
    name = name.split('_')
    return name[0]

def writeMetaData():
    with open('./chunkservers.meta', 'wb') as output:
        pickle.dump(chunkservers, output, pickle.HIGHEST_PROTOCOL)

    with open('./files.meta', 'wb') as output:
        pickle.dump(files, output, pickle.HIGHEST_PROTOCOL)

def readMetaData():
    global chunkservers
    global files

    with open('./chunkservers.meta', 'rb') as ip:
        chunkservers = pickle.load(ip)

    with open('./files.meta', 'rb') as ip:
        files = pickle.load(ip)

class FileInfo:

    def __init__(self, name, totalSize):
        self.name = name
        self.totalSize = int(totalSize)
        self.chunkInfo = {}
        self.hasLastChunk = False
        self.lastChunkID = None
        self.totalChunks = math.ceil(self.totalSize/MAXSIZE)
        self.updateLastChunkStatus()

    def updateLastChunkStatus(self):
        self.hasLastChunk = (self.totalSize%MAXSIZE != 0)
        self.lastChunkID = self.name + "_" + str(math.ceil(self.totalSize/MAXSIZE))

    def updateFileSize(self, size):
        self.totalSize += size
        self.updateLastChunkStatus()

    def updateChunkInfo(self, chunk, cs):
        global chunkservers
        if chunk not in self.chunkInfo.keys():
            self.chunkInfo[chunk] = []
        self.chunkInfo[chunk].append(chunkservers[cs])

    def getLastChunkStatus(self):
        return self.hasLastChunk

    def getTotalSize(self):
        return self.totalSize

    def getTotalChunks(self):
        return self.totalChunks

    def getLastChunkID(self):
        return self.lastChunkID

    def getChunkInfo(self, chunkID):
        if chunkID not in self.chunkInfo.keys():
            return []
        return self.chunkInfo[chunkID]

    def getAllChunkInfo(self):
        return self.chunkInfo

    def getFirstChunkServer(self, chunkID):
        return self.chunkInfo[chunkID][0]

    def removeServerInfo(self, chunk, cs):
        i = 0

        for obj in self.chunkInfo[chunk]:
            if cs[0]==obj.getIP() and cs[1]==obj.getPort():
                break
            i += 1

        self.chunkInfo[chunk].pop(i)

class ChunkServer:

    def __init__(self, ip, port, status):
        self.ip = ip
        self.port = port
        self.status = status
        self.load = 0
        self.chunkInfo = {}

    def addChunk(self, chunk, size):
        self.chunkInfo[chunk] = size
        self.load += 1

    def getStatus(self):
        return self.status

    def setStatus(self, status):
        self.status = status

    def getIP(self):
        return self.ip

    def getPort(self):
        return self.port

    def getChunks(self):
        return self.chunkInfo.keys()

    def updateChunk(self, chunk_list):

        chunks = chunk_list.split(',')

        for cl in chunks:
            c = cl.split(':')
            self.addChunk(c[0], int(c[1]))


class ClientThread(threading.Thread):

    def __init__(self, client_address, client_socket, info):
        threading.Thread.__init__(self)
        self.caddress = client_address
        self.csocket = client_socket
        self.info = info

    def run(self):
        print("Client Connected: ", self.caddress)

        global chunkservers
        global files
        
        msg = ''

        if self.info[0]=='read':
            msg = self.readFile(self.info[1])

        if self.info[0]=='write':
            msg = self.writeFile(self.info[1], int(self.info[2]))

        if self.info[0]=='append':
            msg = self.appendFile(self.info[1], int(self.info[2]))

        self.csocket.sendall(bytes(msg, 'UTF-8'))

        self.csocket.close()

    def readFile(self, name):

        global chunkservers
        global files

        obj = files[name]

        chunkInfo = obj.getAllChunkInfo()

        msg = ''

        for chunk in chunkInfo.keys():
            serverList = ''
            for cs in chunkInfo[chunk]:
                ip = cs.getIP()
                port = cs.getPort()
                serverList += ip + ":" + str(port) + ','
            msg = msg + chunk + "=" + serverList[:-1] + ";"

        msg = msg[:-1]

        return msg


    def writeFile(self, name, size):

        global chunkservers
        global files

        obj = FileInfo(name, size)

        files[name] = obj

        cs_list = list(chunkservers.values())

        cs_list.sort(key=operator.attrgetter('load'))

        i = 0

        j = 0

        msg = ''

        while i < obj.getTotalChunks():

            if j==len(cs_list):
                j = 0

            ip = cs_list[j].getIP()
            port = cs_list[j].getPort()
            writeSize = MAXSIZE

            if i==obj.getTotalChunks()-1:
                writeSize = size%MAXSIZE

            msg += str(ip) + ":" + str(port) + "=" + name + "_" + str(i+1) + ":" + str(writeSize) + ","
            i += 1
            j += 1

        msg = msg[:-1]

        return msg

    def appendFile(self, name, size):

        global chunkservers
        global files

        newSize = size

        obj = files[name]

        cs_list = list(chunkservers.values())

        cs_list.sort(key=operator.attrgetter('load'))

        msg = ''

        if obj.getLastChunkStatus():
            oldSize = obj.getTotalSize()

            lastChunkSize = oldSize%MAXSIZE

            to_add = MAXSIZE-lastChunkSize

            chunkServerList = obj.getChunkInfo(obj.getLastChunkID())
            ip = chunkServerList[0].getIP()
            port = chunkServerList[1].getPort()

            if newSize <= to_add:
                msg = str(ip) + ":" + str(port) + "=" + obj.getLastChunkID() + ":" + str(newSize)
                obj.updateFileSize(size)
                return msg
            else:
                msg = str(ip) + ":" + str(port) + "=" + obj.getLastChunkID() + ":" + str(to_add) + ","
                newSize -= to_add

        i = 0

        j = 0

        lastChunkNumber = int(obj.getLastChunkID().split('_')[1])+1

        total_chunks = math.ceil(newSize/MAXSIZE)

        while i < total_chunks:

            if j==len(cs_list)-1:
                j = 0

            ip = cs_list[j].getIP()
            port = cs_list[j].getPort()
            writeSize = MAXSIZE

            if i==total_chunks-1:
                writeSize = newSize%MAXSIZE

            msg += str(ip) + ":" + str(port) + "=" + name + "_" + str(lastChunkNumber) + ":" + str(writeSize) + ","
            i += 1
            j += 1
            lastChunkNumber += 1

        msg = msg[:-1]

        obj.updateFileSize(size)

        return msg


class HeartbeatThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def chunkServerDown(self, cs):

        global chunkservers
        global files

        # Find which chunks to copy
        chunks_to_copy = chunkservers[cs].getChunks()

        # Make List
        msg = "master:copy:"

        for chunk in chunks_to_copy:

            fileName = getFileName(chunk)

            fileObj = files[fileName]

            fileObj.removeServerInfo(chunk, cs)

            obj = fileObj.getFirstChunkServer(chunk)

            msg += obj.getIP() + ":" + str(obj.getPort()) + "=" + chunk + ","

        msg = msg[:-1]

        chunkservers[cs].setStatus(False)

        del chunkservers[cs]

        # Send this list to one of the chunkservers
        cs_list = list(chunkservers.values())

        cs_list.sort(key=operator.attrgetter('load'))

        cs = cs_list[0]

        send_ip = cs.getIP()
        send_port = cs.getPort()

        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect((send_ip, send_port))
        sender.sendall(bytes(msg, 'UTF-8'))
        sender.close()
        writeMetaData()

    def run(self):
        global chunkservers
        global files
        global primaryStatus

        if primaryStatus:
            healthcheck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                healthcheck.connect((HOST, PRIMARY_PORT))
            except socket.error:
                try:
                    healthcheck.connect((HOST, PRIMARY_PORT))
                except socket.error:
                    primaryStatus = False

            if primaryStatus:
                healthcheck.sendall(bytes("healthcheck", 'UTF-8'))
                data = healthcheck.recv(2048)
                if not data:
                    primaryStatus = False
            print("Primary Master Status: ", primaryStatus)
            healthcheck.close()

        if not primaryStatus:
            readMetaData()

            for cs in list(chunkservers):
                if chunkservers[cs].getStatus():
                    ip = cs[0]
                    port = cs[1]
                    heartbeat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    check = False
                    try:
                        heartbeat.connect((ip, port))
                    except socket.error:
                        try:
                            heartbeat.connect((ip, port))
                        except socket.error:
                            print("Unable to connect to ChunkServer ", cs)
                            check = True
                    if not check:
                        heartbeat.sendall(bytes("master:heartbeat", 'UTF-8'))
                        data = heartbeat.recv(2048)
                    heartbeat.close()
                    if check:
                        print(cs, " chunkserver is down, copying chunks to other chunkserver")
                        self.chunkServerDown(cs)
            print("Heartbeat Round Completed")

        threading.Timer(20, self.run).start()

class RegisterChunkServerThread(threading.Thread):

    def __init__(self, chunk_address, chunk_socket):
        threading.Thread.__init__(self)
        self.csocket = chunk_socket
        self.caddress = chunk_address

    def run(self):
        print("RegisterChunkServerThread Started")

        global chunkservers
        global files

        self.csocket.sendall(bytes("ok", 'UTF-8'))
        data = self.csocket.recv(20000)

        if data:
            data = data.decode().split(',')
            ip = self.caddress[0]
            port = self.caddress[1]

            obj = ChunkServer(ip, port, True)

            for d in data:
                chunk_info = d.split(':')
                obj.addChunk(chunk_info[0], int(chunk_info[1]))

            chunkservers[self.caddress] = obj

            for d in data:
                chunk_info = d.split(':')

                chunkName = chunk_info[0]

                fileName = getFileName(chunkName)

                fileObj = files[fileName]

                fileObj.updateChunkInfo(chunkName, self.caddress)
        else:
            ip = self.caddress[0]
            port = self.caddress[1]
            obj = ChunkServer(ip, port, True)
            chunkservers[self.caddress] = obj

        self.csocket.close()

        print("RegisterChunkServerThread Completed")

class InfoThread(threading.Thread):

    def __init__(self, chunk_address, chunk_socket, chunk_name):
        threading.Thread.__init__(self)
        self.csocket = chunk_socket
        self.caddress = chunk_address
        self.cname = chunk_name

    def run(self):
        print("InfoThread Started")

        global chunkservers
        global files

        fileName = getFileName(self.cname)

        fileObj = files[fileName]

        chunk_server_info = fileObj.getChunkInfo(self.cname)

        cs_list = list(chunkservers.values())

        cs_list.sort(key=operator.attrgetter('load'))
        
        i = 0
        while len(chunk_server_info) < 3 and i < len(cs_list):
            if cs_list[i] not in chunk_server_info:
                chunk_server_info.append(cs_list[i])
            i += 1

        msg = ''

        for obj in chunk_server_info:
            msg += obj.getIP() + ":" + str(obj.getPort()) + ','

        msg = msg[:-1]

        self.csocket.sendall(bytes(msg, 'UTF-8'))
        self.csocket.close()

        print("InfoThread Completed")

class UpdateThread(threading.Thread):

    def __init__(self, address, sock, ip, port):
        threading.Thread.__init__(self)
        self.caddress = address
        self.csocket = sock
        self.cip = ip
        self.cport = port

    def run(self):
        print("UpdateThread Started")

        global chunkservers
        global files

        cs = (self.cip, int(self.cport))

        self.csocket.sendall(bytes("ok", 'UTF-8'))

        data = self.csocket.recv(2048)

        chunkservers[cs].updateChunk(data.decode())
        for cl in data.decode().split(','):
            chunk_info = cl.split(':')
            chunkName = chunk_info[0]
            fileName = getFileName(chunkName)

            fileObj = files[fileName]

            fileObj.updateChunkInfo(chunkName, cs)

        print("UpdateThread Completed")
        writeMetaData()
        

if __name__ == '__main__':
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, SECONDARY_PORT))

    print("Secondary Server started")
    print("Waiting for requests..")

    HeartbeatThread().start()
    msg = ''
    while True:
        server.listen()
        sock, address = server.accept()
        data = sock.recv(2048)
        msg = data.decode()
        if msg == 'register':
            RegisterChunkServerThread(address, sock).start()
        if msg == 'healthcheck':
            sock.sendall(bytes("ok", 'UTF-8'))
        words = msg.split(':')
        if words[0] == 'info':
            InfoThread(address, sock, words[1]).start()
        elif words[0] == 'client':
            ClientThread(address, sock, words[1:]).start()
        elif words[0] == 'update':
            UpdateThread(address, sock, words[1], words[2]).start()