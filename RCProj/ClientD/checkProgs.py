import threading
import argparse
import socket
import logging
import time,os
from collections import namedtuple

from ..Config.cloudconfig import def_config
from ..Config.cloudconfig import protocol

import getOutput

PROTOC = protocol()

# extract workers information from a specified input file.
# typically that file should locate besides the input file
def record2workers(ifile):
    workers = []
    with open(ifile,'r') as recordfile:
        linenum=0
        
        for line in recordfile:
            linenum +=1 
            worker_parts = line.split(def_config.log_delimiter)
            if(len(worker_parts) >=5):
                temp_worker = {}
                temp_worker[def_config.worker.client_tasks_id] = worker_parts[0]
                temp_worker[def_config.worker.worker_ip] = worker_parts[1]
                temp_worker[def_config.worker.worker_port] = int(worker_parts[2])
                temp_worker[def_config.worker.task_log_idx] = int( worker_parts[3])
                temp_worker[def_config.worker.key] = worker_parts[4]
                workers.append(temp_worker)
            else:
                print('The record file for workers %s is corrupted at %d'%(recordfile,linenum))
    
    return workers

# if user press enter, should exit the pinging loop
def check_user_input(cross_end_sign):
    while not cross_end_sign['tasks_all_end']:
        i = raw_input('Press Enter to exit the checking loop')
        if not i:
            cross_end_sign['user_end'] = True
            #print('cross_end_sign:%s'%cross_end_sign) 
            break
        print('You input :',i) 
    
# show the progress and change corresponding stat
def chgst_show_prog(worker, reply):
    if reply[PROTOC.attr.prog_st] == PROTOC.prog_st.completed:
        
        worker[def_config.worker.prog_st] = PROTOC.prog_st.completed
        worker[def_config.worker.ab_taskout_path] = reply[PROTOC.attr.ab_taskout_path]
        worker[def_config.worker.sock].close()

        print('===%s | %s completed its task'%
                ( worker[def_config.worker.client_tasks_id]
                    ,worker[def_config.worker.worker_ip]
                    )  )
    elif reply[PROTOC.attr.prog_st] == PROTOC.prog_st.not_found:
        worker[def_config.worker.prog_st] = PROTOC.prog_st.not_found
        worker[def_config.worker.sock].close()

        if reply[PROTOC.attr.is_found_tasklog] == True:
            print('===%s | %s lost result'%
                    (worker[def_config.worker.client_tasks_id]
                        ,worker[def_config.worker.worker_ip]))
            logging.warning('checkProgs:check1:No result in Output dir.Exist Log in server tasklog. try resubmit')
        else:
            logging.warning('checkProgs:check1:No result in Output dir,No log in server \
                            tasklog. pls ensure the record is correct. %s', workers[idx])
            print('===%s | %s cant locate requested task in its tasklog'%
                    (worker[def_config.worker.client_tasks_id]
                        ,worker[def_config.worker.worker_ip]))
    elif reply[PROTOC.attr.prog_st] == PROTOC.prog_st.current_thread_executing:
        worker[def_config.worker.prog_st] = PROTOC.prog_st.current_thread_executing
        logging.info('%s is cureently processing task %s '
                        ,worker[def_config.worker.worker_ip]
                        ,worker[def_config.worker.client_tasks_id])
        print('=== %s | %s:%s progs | %d/%d is completed' 
                %(worker[def_config.worker.client_tasks_id]
                    ,worker[def_config.worker.worker_ip]
                    ,worker[def_config.worker.worker_port]
                    ,reply[PROTOC.attr.done_remain][0]
                    ,reply[PROTOC.attr.done_remain][0]+ reply[PROTOC.attr.done_remain][1]) )

def check_task_progs(workers,ping_int):

    cross_end_sign = {}
    cross_end_sign['user_end'] = False
    cross_end_sign['tasks_all_end'] = False
   
    #for idx in range(len(workers)):
    # iterate check_progs all servers to get the result
    for idx in range(len(workers)):
        temp_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        # save that handler for later stage
        workers[idx][def_config.worker.sock]= temp_sock
        
        server_address = (workers[idx][def_config.worker.worker_ip]
                        ,workers[idx][def_config.worker.worker_port])
        print('[Check Progs] Connecting to %s port %s'%server_address)
        try:
            temp_sock.connect(server_address)
            message= PROTOC.gen_check_prog_msg(workers[idx][def_config.worker.client_tasks_id],
                   workers[idx][def_config.worker.task_log_idx])

            PROTOC.send_msg(temp_sock,message)
            replyJson = PROTOC.recv_msg(temp_sock)
            reply = PROTOC.parse(replyJson)
            
            chgst_show_prog(workers[idx],reply)

        finally:
            #TODO, leave structure for handling errors
            # also should have a check server stat before this step probably,
            pass 
    
    # collect the check result and 
    # if task is completed or not found, close the sock
    # if task is currently processing, keeps pinging until user 'enter' or server completed.
    cross_end_sign['tasks_all_end'] = True
    for idx in range(len(workers)):
        if workers[idx][def_config.worker.prog_st] != PROTOC.prog_st.current_thread_executing :
            cross_end_sign['tasks_all_end'] &= True
        else:
            cross_end_sign['tasks_all_end'] &= False

    if cross_end_sign['tasks_all_end'] == False:
        print('start pinging the workers to get result')
        # start monitoring user 'enter' loop
        thd_user = threading.Thread(target=check_user_input,args=(cross_end_sign,))
        thd_user.start()
        last_time = time.time()

        while not cross_end_sign['tasks_all_end']  and not cross_end_sign['user_end']:
            if time.time() - last_time > ping_int:
                cross_end_sign['tasks_all_end'] = True
                for worker in workers:
                    if worker[def_config.worker.prog_st] == PROTOC.prog_st.current_thread_executing:
                        # ping and get feed reply
                        messageJson = PROTOC.gen_ping_msg()
                        PROTOC.send_msg(worker[def_config.worker.sock],messageJson)
                        replyJson = PROTOC.recv_msg(worker[def_config.worker.sock])
                        reply = PROTOC.parse(replyJson)
                        chgst_show_prog(worker,reply)
                        # if reply is server still processing, 
                        if worker[def_config.worker.prog_st] == PROTOC.prog_st.current_thread_executing:
                            cross_end_sign['tasks_all_end'] &=False 
                if cross_end_sign['tasks_all_end'] == False:
                    print('Tasks is still not totally completed yet. wait %d sec and start new loop'
                            %ping_int)
                    last_time = time.time()
                else:
                    break
            else:
                pass 

        
        # if we break the checking loop by user enter
        print(cross_end_sign)
        if cross_end_sign['user_end'] == True:
            print('Tasks is not totally completed yet, check them later.')
            for worker in workers:
                if worker[def_config.worker.prog_st] != PROTOC.prog_st.completed:
                    # break loop from client side
                    endJson = PROTOC.gen_end_msg()
                    PROTOC.send_msg(worker[def_config.worker.sock],endJson)
                    
                    print('%s == %s is %s' %
                            (worker[def_config.worker.client_tasks_id]
                                ,worker[def_config.worker.worker_ip]
                                ,worker[def_config.worker.prog_st]) )
        elif cross_end_sign['tasks_all_end'] == True:
            print('All tasks end, press enter to exit loop.')
            thd_user.join()
        else:
            print('Fatal error !!! Wrong quit loop logic!!! debug your code')
    else:
        print('All tasks end')
    # wait until user finally press a enter
    print('cross_end_sign:%s'%cross_end_sign) 
    print('Show workers stat:')
    for worker in workers:
        print worker
    return workers 


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--ifile')
    parser.add_argument('-pi','--pingint')
    args=parser.parse_args()

    if(args.ifile == None ):
        print('Essential ifile is missing.')
        print('python checkProgs.py -i ifile [-pi pingint]')
        exit()
    # ping_int decide the time interval between two ping messages
    ping_int = PROTOC.ping_interval 
    if args.pingint != None:
        ping_int = args.pingint
    

    workers=record2workers(args.ifile)
    if(len(workers) == 0):
        print('specified input file(%s) contains not valid server/worker information'
                %args.ifile)
        exit()

    # check the progress 
    workers = check_task_progs(workers,ping_int)
    parent_dir = os.path.abspath(os.path.join(args.ifile,os.pardir))
    getOutput.get_output_socket(workers,'./')
    
    #key = None
    #workers =  
    
