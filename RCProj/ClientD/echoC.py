import socket,errno
import sys
import argparse
import threading
import logging
from collections import namedtuple

from ..Config.taskmaster import taskmaster
from ..Config.cloudconfig import def_config
from ..Config.cloudconfig import *

import checkIP

# Initialization the configuration information
SSTAT = server_status()
PROTOC = protocol()

Worker = namedtuple('Worker',['ip','ab_projs_dir'])
worker = Worker('ip','ab_projs_dir')



# external function called in the server side
ExtnFunction = 'echo'

# ***** helper functions *****

# After succefully assign the tasks, save the information about workers to a file
# That file would just next to args.ifile.
def save_workers2record(args_ifile, workers,TasksID):
    parent_dir = os.path.abspath(os.path.join(args_ifile,os.pardir))
    #print('parent dir path of input file: %s'%p_dir)
    task_record_file = parent_dir + '/'+TasksID +'.record'
    with open(task_record_file,'w') as recordfile:
        for worker in workers:
            str_log = worker['client_tasks_id'] + def_config.log_delimiter \
                    + worker['ip'] + def_config.log_delimiter \
                    + str( worker['task_log_idx'] ) + def_config.log_delimiter \
                    + '\n'
            recordfile.write(str_log)
    logging.info('save info about workers at %s',task_record_file)


# assign tasks as bulks
def assign_tasks_bulk(inputs_dict):
    # get arguments back from dictory. This is tedious. 
    # there should be a better way to deal with things like that
    args_ifile = inputs_dict['ifile']
    args_proj = inputs_dict['proj']
    args_exfun = inputs_dict['exfun'] 
    args_test = inputs_dict['test'] 
    args_walltime = inputs_dict['walltime'] 
    args_tasks_id = inputs_dict['tasks_id'] 
    
    # ***** initialize variables here *****
    Walltime = args_walltime 
    TasksID = args_tasks_id
    if args_exfun !=None:
        ExtnFunction = args.exfun

    # initialization the task master.
    task_master = taskmaster(args_ifile,args_proj)
    task_master.extract_tasks()
    
    avail_ips = checkIP.get_avail_ips(args_proj,args_test)

    if len(avail_ips) == 0:
        print('No server available for the proj %s right now. might try later'%args.proj)
        exit()
    task_master.chunk_tasks(len(avail_ips))

    # ****** Initialization  End here ************

    # prepare a worker list to store the essential infomation
    workers = []
    for avail_ip in avail_ips:
        worker = {}
        worker['ip'] = avail_ip
        worker[PROTOC.attr.client_tasks_id] = TasksID
        workers.append(worker)
    
    # ***** Actual assigning tasks here ********
    # for each avail ip assign it a task list
    for idx in range(len(avail_ips)):
        # First, check the server again
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        server_address = (avail_ips[idx],def_config.default_port)
        sock.connect(server_address)

        try:
            # 
            msg_json = PROTOC.gen_req_stat_msg(args_proj)
            PROTOC.send_msg(sock,msg_json)
            reply_json = PROTOC.recv_msg(sock)
            reply = PROTOC.parse(reply_json)
            print('Received : %s'%reply)
        finally:
            sock.close()
       
        # Second, send tasks as request to the avail server
        if(reply[PROTOC.attr.server_stat] == SSTAT.status.ready):
            # recreate the socket 
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            server_address = (avail_ips[idx],def_config.default_port)
            sock.connect(server_address)
            try:
                # essential fix project path and output path
                task_master.project_path = reply[PROTOC.attr.ab_projs_dir] + '/' + args.proj
                task_master.output_dir =  task_master.project_path + '/Output'
                sub_task_list = task_master.get_idx_chunk(idx,ExtnFunction)
                print('%s get %d tasks '%(avail_ips[idx],len( sub_task_list ) ))
                
                # Exchange information 
                req_json = PROTOC.gen_req_task_msg(TasksID, sub_task_list,Walltime)
                PROTOC.send_msg(sock,req_json)
                reply_json = PROTOC.recv_msg(sock)
                reply = PROTOC.parse(reply_json)
                
                # if the back message contain a task_log_idx that is not 0
                # means server accepted the tasks.
                if( reply[PROTOC.attr.client_tasks_id] == TasksID 
                        and reply[PROTOC.attr.task_log_idx]!= 0 ):
                    task_log_idx = reply[PROTOC.attr.task_log_idx]
                    print('%s starts doing tasks as number %d task in its log'
                            %(avail_ips[idx],task_log_idx))
                    
                    #  Record these infomation somewhere
                    workers[idx][PROTOC.attr.task_log_idx] = task_log_idx

                else:
                    print('%s refuse to work on sub_task_list. Tasks below need redo:'%avail_ips[idx])
                    logging.warning('TaskID back is wrong. %s refuse to work on these tasks. Tasks below need redo.',avail_ips[idx])
                    for task in sub_task_list:
                        print(task)
                        logging.warning(task)

            finally:
                sock.close()
        else:
            # If the server is not ready for tasks. show user the undo tasks
            print('%s is now busy. cant assign tasks'%avail_ips[idx])
            logging.debug('%s is busy for tasks id - %s',avail_ips[idx],TasksID)
            logging.warning('these tasks need redo: ')
            
            sub_task_list = task_master.get_idx_chunk(idx,ExtnFunction)
            for task in sub_task_list:
                logging.warning(task)

    save_workers2record(args_ifile,workers,TasksID)
    return workers


# ***** Handle input/test part *****
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--ifile')
    parser.add_argument('-p','--proj')
    parser.add_argument('-ef','--exfun')
    parser.add_argument('-t','--test')
    parser.add_argument('-wt','--walltime')
    parser.add_argument('-id','--tasks_id')
    args=parser.parse_args()

    # check input 
    if(args.ifile == None or args.proj == None or args.exfun == None or args.tasks_id==None):
        print('Essential information missing.')
        print('-id tasks_id -i inputfile -p project -ef external function')
        exit()
    
    # put all the input arguments to a dict
    inputs_dict={}
    inputs_dict['ifile'] = args.ifile
    inputs_dict['proj'] = args.proj
    inputs_dict['exfun'] = args.exfun
    inputs_dict['test'] = args.test
    inputs_dict['walltime'] = args.walltime
    inputs_dict['tasks_id'] = args.tasks_id
    
    Workers = assign_tasks_bulk(inputs_dict)
    print Workers




