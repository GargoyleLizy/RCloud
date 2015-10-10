import socket,errno
import sys
import argparse
import threading

from ..Config.cloudconfig import def_config
from ..Config.cloudconfig import *

# Initialization the configuration information
SSTAT = server_status()
PROTOC = protocol()

def fetch_workers(server_file):
    print('fetch workers at %s'%server_file)
    workers = []
    linenum = 0
    with open(server_file,'r') as worker_file:
        for line in worker_file:
            linenum +=1
            worker_parts = line.split(def_config.log_delimiter)
            if worker_parts >= 3:
                temp_worker = {}
                temp_worker[def_config.worker.worker_ip] = worker_parts[0]
                if worker_parts[1] != 'None':
                    temp_worker[def_config.worker.worker_port] = int(worker_parts[1])
                else:
                    temp_worker[def_config.worker.worker_port] = def_config.default_port
                
                temp_worker[def_config.worker.key] = worker_parts[2] 
                
                workers.append(temp_worker)
            else:
                print('server file %s is corrupted at line %d'%(server_file,linenum)) 
    return workers

def check_servers(workers,proj):
    # return the servers available for that proj
    thds = []
    results=[None]*len(workers)
    avail_workers = []
    # check servers parallel
    for idx in range(len(workers)):
        temp_worker = workers[idx]
        thd = threading.Thread(target=check_one_server,args=(temp_worker,proj,results,idx))
        thds.append(thd)

    for thd in thds:
        thd.start()
    for thd in thds:
        thd.join()
    
    for idx in range(len(workers)):
        temp_worker = workers[idx]
        #print(server_ip,results[idx])
        if results[idx] == 'avail':
            avail_workers.append(temp_worker)
        else:
            pass

    print('available servers are : %s'%avail_workers)
    return avail_workers 

def check_one_server(worker,proj,results,idx):
    # check one server. 
    temp_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server_address = (worker[def_config.worker.worker_ip], worker[def_config.worker.worker_port])
    print('[Checking] Connecting to %s port %s'%server_address)
    try:
        
        temp_sock.connect(server_address)
       
        temp_sock.settimeout(PROTOC.msg_timeout)
        message = PROTOC.gen_req_stat_msg(proj)
        
       
        PROTOC.send_msg(temp_sock,message)
        #print('[Send] to %s: \"%s\" '%(worker[def_config.worker.worker_ip],message))
        temp_reply = PROTOC.recv_msg(temp_sock) 
        # # recv , considered timeout
        # #temp_reply=PROTOC.recv_msg(temp_sock)
        # for _ in range(PROTOC.timeout_retries):
        #     try:
        #         temp_sock.settimeout(PROTOC.msg_timeout)
        #         #temp_sock.setblocking(1) 
        #         #temp_reply = temp_sock.recv(PROTOC.buff_size)
        #         temp_reply = PROTOC.recv_msg(temp_sock)
        #         break 
        #     except socket.timeout:
        #         print("caught a timeout")
        #         pass
        
        # check reply
        if temp_reply !=None:
            #print('[Recv] from %s: %s'%(worker[def_config.worker.worker_ip],temp_reply))
            reply = PROTOC.parse(temp_reply)
            #print(reply)
            if(reply[PROTOC.attr.msg_type]==PROTOC.reply.req_stat):
                if(reply[PROTOC.attr.server_stat] == SSTAT.status.ready):
                    if(reply[PROTOC.attr.proj] == True):
                        results[idx] = 'avail'
                    else:
                        results[idx] = 'no proj'
                else:
                    results[idx] = 'server busy'
                
        else:
            print('Finally timeout')
            results[idx] = 'Timeout Error'
        

    except socket.error, e:
        if isinstance(e.args, tuple):
            logging.debug( "errno is %d" ,e[0] )
            logging.debug(e)
            if e[0] == errno.EPIPE:
               # remote peer disconnected
               logging.debug("Detected remote disconnect")
            else:
               # determine and handle different error
               pass
        else:
            print "socket error ", e
        results[idx] = 'Failed connection'
    finally:
        temp_sock.close()

def get_total_workers(args_serverfile):
    workers = []
    # decide the workers
    if args_serverfile == None:
        print('check local host ')
        host_name = 'localhost'
        test_worker = {}
        test_worker[def_config.worker.worker_ip] = host_name
        test_worker[def_config.worker.worker_port] = def_config.default_port
        test_worker[def_config.worker.key] = 'None'
        workers.append(test_worker)
    else:
        workers = fetch_workers(args_serverfile)
        print workers
        #server_ips = fetch_server_ips()
        


    return workers

# ***** Handle input/test part *****
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--proj')
    parser.add_argument('-sf','--server_file')
    args=parser.parse_args()
    if(args.proj == None ):
        print('Essential proj is missing')
        exit()
    
    total_workers = get_total_workers(args.server_file)
    # Check if the server available. and if the requested project available in that server 
    workers = check_servers(total_workers,args.proj)
    
