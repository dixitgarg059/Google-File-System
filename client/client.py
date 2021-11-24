import socket
import os
import pickle
import sys
import math, random

MASTER_SV_PORT = 8080
MASTER_SV_BACKUP_PORT = 8081

#Connecting to the master_server
def connect_to_master_server(getCommand, no_of_arg):
    try:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(("127.0.0.1",MASTER_SV_PORT))
    except:
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),MASTER_SV_BACKUP_PORT))
        except:
            print("Master server is not active...Try again later!!!")        
            sys.exit()

    decision = getCommand.split(' ')[0]
    print("Connecting to master to get metadata")
    if no_of_arg==2:
        filename = getCommand.split(' ')[1]
        if(decision=="write"):
            size=str(os.path.getsize(filename))
            fileplussize="client"+":write:"+filename+":"+size
            s.send(fileplussize.encode("ascii"))
            status=s.recv(2048).decode("ascii")
            s.close()
            print("Metadata Received")
            return status
            
        if(decision=="read"):
            file_read= "client"+":read:"+filename
            s.send(file_read.encode("ascii"))
            status = s.recv(2048).decode("ascii")
            s.close()
            print("Metadata Received")
            return status
       
    if(decision == "append"):
        decision, fileto, filefrom = getCommand.split(' ')
        size=str(os.path.getsize(filefrom))
        fileplussize="client"+":append:"+fileto+":"+size
        s.send(fileplussize.encode("ascii"))
        status=s.recv(2048).decode("ascii")
        s.close()
        print("Metadata Received")
        return status
            

#Connecting to the chunk_server
def send_to_chunk_server(decision,chunkInfo,filename):
    print("Connecting to respective chunk server")
    if(decision=="read"):
        info_arr = chunkInfo.split(";")
        for files in info_arr:
            filename_chunkid = files.split("=")[0]
            ip_ports = files.split("=")[1]
            ip_port = ip_ports.split(",")[0]
            ip = ip_port.split(":")[0]
            port = int(ip_port.split(":")[1])
            chunk_server_msg = "client:" + "read:" + filename_chunkid
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((ip,port))
            s.send(chunk_server_msg.encode('ascii'))
            print("Reading "+filename_chunkid+".....")
            status = s.recv(2048)
            s.close()
        return

    if(decision=="append") or (decision=="write"):
        if (decision == "append"):
            filefrom = filename[1]
            filename = filename[0]
        else:
            filefrom = filename
        ip_port_arr = chunkInfo.split(",")
        chunk = "./"+filefrom
        with open(chunk, 'rb') as f:
            for ip_port_size in ip_port_arr:
                ip_port, write_file_size = ip_port_size.split("=")
                ip, port = ip_port.split(":")
                filename_chunkid, writeSize = write_file_size.split(":")
                port = int(port)
                s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect((ip,port))
                chunk_server_msg = "client:" + "append:" + filename_chunkid + ":" + writeSize
                s.send(chunk_server_msg.encode('ascii'))
                status = s.recv(100)
                data=f.read(int(writeSize))
                s.sendall(data)
                s.close()
        return

            
if __name__=="__main__":
    print("Enter commands in the following format: ")
    print("\tTo upload a file: write <filename>")
    print("\tTo read a file: read <filename>")
    print("\tTo append some data in a file: append <tofile> <fromfile>")
    while True:
        getCommand=input(">> ")
        a=len(getCommand.split())

        decision = getCommand.split(' ')[0]
        if a==2:
            filename = getCommand.split(' ')[1]
            if(decision=="write"):
                chunkInfo=connect_to_master_server(getCommand,a)
                if chunkInfo:
                    send_to_chunk_server(decision,chunkInfo,filename)
                    print("File Written Successfully")
                else:
                    print("File already Present")    
            
            if(decision=="read"):
                chunkInfo=connect_to_master_server(getCommand,a)
                if chunkInfo:
                    send_to_chunk_server(decision,chunkInfo,filename)
                    print("File Read Successfully")
                else:
                    print("File Read is Unsuccessful")


        if (decision=="append"):
            decision, fileto, filefrom = getCommand.split(' ')
            chunkInfo=connect_to_master_server(getCommand,a)
            if chunkInfo:
                send_to_chunk_server(decision,chunkInfo,[fileto,filefrom])
                print("File Appended Successfully")
            else:
                print("File not Appended")