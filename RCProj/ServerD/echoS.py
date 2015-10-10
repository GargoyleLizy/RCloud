import socket
import sys,os,logging
import argparse
from ..Config.taskmaster import taskmaster
#from ..Config.cloudconfig import def_config
from ..Config.cloudconfig import *

# Initialization the configuration information
CONFIG = def_config()
SSTAT = server_status()
PROTOC = protocol() 

# ***** some helper functions.*******
# check if the server side projects are set appropriately. 
def check_projs_installed(temp_cloud_dir):
    ab_cloud_dir = os.path.abspath(temp_cloud_dir)
    os.chdir(ab_cloud_dir)
    # check if Projs exist
    top_dirs=[ name for name in os.listdir('./') if os.path.isdir(os.path.join('./', name))]
    print(ab_cloud_dir)
    print("Top modules: %s "% top_dirs)
    if SSTAT.projs_dir in top_dirs:
        print('Find Projects directory. List available projects below:')
        ab_projs_dir = ab_cloud_dir + '/' + SSTAT.projs_dir
        os.chdir(ab_projs_dir)

        top_projs=[ name for name in os.listdir('./') 
                if os.path.isdir(os.path.join('./', name))]
        print('Avail Projs: %s'%top_projs)
        #return (None,None,None)
        return (ab_cloud_dir,ab_projs_dir,top_projs) 
    else:
        return (None,None,None)



def dowork():
    print("I am doing the work, this code not completed yet")

def empty_message_error():
    print('message is empty')


# search the tasks output in the Output/. If cannot find corresponding result
# try tasklog and figure out what is wrong.
def check_progs_in_past(SSTAT,PROTOC,client_tasks_id,task_log_idx,sock):
    # find the result in past tasks
    logging.debug('Enter check_progs_in_past')
    
    output_file_name = str( task_log_idx ) + def_config.log_delimiter + client_tasks_id + '.zip'
    found_flag = False

    os.chdir(SSTAT.ab_taskout_dir)
    for f in os.listdir(SSTAT.ab_taskout_dir):
        if output_file_name == f :
            print('%s is requested task output. '%output_file_name)
            logging.debug('Found target result %s',output_file_name)
            found_flag = True
            break

    message = None
    if found_flag :
        message = PROTOC.gen_check_prog_Out_rep(found_flag,None, os.path.join(SSTAT.ab_taskout_dir,output_file_name))
    else:
        # search tasklog and try to find the task
        is_found_intasklog = SSTAT.search_tasklog(client_tasks_id,task_log_idx)
        message = PROTOC.gen_check_prog_Out_rep(False,is_found_intasklog,None)
    PROTOC.send_msg(sock,message)

def check_progs_inter(SSTAT,PROTOC, message,sock):
    # for check_progs
    client_tasks_id = message[PROTOC.attr.client_tasks_id]
    task_log_idx = message[PROTOC.attr.task_log_idx]
    
    logging.debug('Enter check_progs_inter') 

    # check current task
    if(SSTAT.current_thread != None):
        logging.debug('current_thread : %s + %d'%(SSTAT.current_thread.client_tasks_id,SSTAT.current_thread.task_log_idx))
        if ( SSTAT.current_thread.client_tasks_id == client_tasks_id and 
            SSTAT.current_thread.task_log_idx == task_log_idx):
            # If the requested task is current thread task
            done_count = SSTAT.current_thread.get_taskdone_count()
            remain_count = SSTAT.current_thread.get_remain_count()
            reply = PROTOC.gen_check_prog_curentT_rep(done_count,remain_count)
            PROTOC.send_msg(sock,reply)
            # keeps pinging and checking until one finished.
            while True: 
                data = PROTOC.recv_msg(sock)
                pingMsg = PROTOC.parse(data)
                if pingMsg[PROTOC.attr.msg_type] == PROTOC.request.ping:
                    if(SSTAT.current_thread != None):
                        if ( SSTAT.current_thread.client_tasks_id == client_tasks_id and 
                            SSTAT.current_thread.task_log_idx == task_log_idx):
                            done_count = SSTAT.current_thread.get_taskdone_count()
                            remain_count = SSTAT.current_thread.get_remain_count()
                            reply = PROTOC.gen_check_prog_curentT_rep(done_count,remain_count)
                            PROTOC.send_msg(sock,reply)
                        else:
                            # thread is changed , Though this is very unlikely. 
                            # try to find the result in the past tasks
                            check_progs_in_past(SSTAT,PROTOC,client_tasks_id,task_log_idx,sock)
                            logging.debug('Current thread is no longer processing the task, try to serch result in Output')
                            break
                    else:
                        # current thread is ended
                        # try to find the result in the past tasks
                        logging.debug('Current task is just completed, try to search result in Output')
                        check_progs_in_past(SSTAT,PROTOC,client_tasks_id,task_log_idx,sock)
                        break
                elif pingMsg[PROTOC.attr.msg_type] == PROTOC.request.end:
                    # the client side end the stream, break and end.
                    logging.debug('Received end signal from client, break the communication')
                    break
                else:
                    # cannot recognize the message received, show an error and break,(or pass?)
                    logging.warnning('echoS:check_progs_inter: Received message is not ping or end, abort message and quit.')
                    break 
        else:
            # Current task is not the requested task, try to find out in past tasks
            logging.debug('current tasks is not requested task, try check_progs_in_past ')
            check_progs_in_past(SSTAT,PROTOC,client_tasks_id,task_log_idx,sock)
    else:
        # try to find the result in the past tasks
        logging.debug('current thread is None, try check_progs_in_past')
        check_progs_in_past(SSTAT,PROTOC,client_tasks_id,task_log_idx,sock) 

def check_output_file(SSTAT,requested_file):
    os.chdir(SSTAT.ab_taskout_dir)
    for f in os.listdir(SSTAT.ab_taskout_dir):
        ab_f_path = os.path.join(SSTAT.ab_taskout_dir,f)
        if requested_file == ab_f_path :
            print('%s is requested task output. '%ab_f_path)
            logging.debug('Found target result %s',ab_f_path)
            return True
    return False




# ***** Initialization before running *****

# Get the server directory path 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-cd','--cdir')
    parser.add_argument('-t','--test')
    parser.add_argument('-p','--port')
    args=parser.parse_args()

    logging.basicConfig(filename='server_log',level=logging.DEBUG)

# ****** A lot of checking/Preparation here *****
# check if the project directories are set correctly
temp_cloud_dir = args.cdir
(ab_cloud_dir,ab_projs_dir,projs_list) = check_projs_installed(temp_cloud_dir)
if(ab_projs_dir == None ):
    logging.warning("project directory on the server side is not available. quit")
    exit()
# SSAT record info
SSTAT.set_ab_cloud_dir(ab_cloud_dir)
SSTAT.set_ab_projs_dir(ab_projs_dir)
SSTAT.set_projs_list(projs_list)

# check the log of taks done 
last_log_idx = SSTAT.check_taskslog()
logging.info('last job index is %d'%last_log_idx)


if args.test !=None:
    host_name ='localhost'
else:
    # get the local ip name using gmail.com.... what the heck
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("gmail.com",80))
    host_name = s.getsockname()[0]
    s.close()

if args.port != None:
    host_port = int(args.port)
else:
    host_port = CONFIG.default_port

# SSAT record info
SSTAT.set_ip_port(host_name,host_port)
print('Start server at %s:%d'%(host_name,host_port))

# ****** Service Loop starts from Here *****

# create a tcp/ip socket
sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
server_address = (SSTAT.host_name, SSTAT.port)
logging.info( 'starting up on %s port %s' % server_address  )
sock.bind(server_address)

# listen for incoming connections
sock.listen(1)

while True:
    print('waiting for a connection')
    connection,client_address=sock.accept()
    try:
        print('Connecting from %s'% (client_address,))
        #data = connection
        data = PROTOC.recv_msg(connection)
        print('Received: %s'%data)
        #data2 = connection.recv(PROTOC.buff_size)
        if data:
            message = PROTOC.parse(data)
            
            if message[PROTOC.attr.msg_type] == PROTOC.request.req_stat:
                # For  req_stat
                # if server is not busy and requested proj is available 
                # return a true for client
                req_proj = message[PROTOC.attr.proj]
                proj_is_avail =False
                if req_proj in SSTAT.projs_list:
                    proj_is_avail = True

                reply = PROTOC.gen_req_stat_rep(SSTAT.get_curent_status()
                                ,proj_is_avail,SSTAT.ab_projs_dir)
                logging.info("Send \"%s\" to %s"%(reply,client_address))
                #connection.sendall(reply)
                PROTOC.send_msg(connection,reply)
            
            elif message[PROTOC.attr.msg_type] == PROTOC.request.req_task:
                # for req_task
                # if server is not busy right now, take the work and send a reply
                # the serverstatus would process the work in a seperate thread
                # else send a -1 indicate that the work cant be done.
                client_tasks_id = message[PROTOC.attr.client_tasks_id]
                task_list = message[PROTOC.attr.task_list]
                walltime = message[PROTOC.attr.walltime]
                #print(client_tasks_id,task_list,walltime)

                if SSTAT.get_curent_status() != SSTAT.status.ready:
                    msg_json = PROTOC.gen_req_task_rep(client_tasks_id)
                else:
                    # Add the log to task log
                    task_log_idx = SSTAT.add_task_log(client_tasks_id,task_list,walltime)    
                    msg_json = PROTOC.gen_req_task_rep(client_tasks_id,task_log_idx)
                    logging.info('Server Take tasks:%s',client_tasks_id)

                    # open a thread in SSTAT 
                    SSTAT.execut_task_list(task_list,client_tasks_id,task_log_idx,walltime) 
                logging.info('Send \'%s\' to %s'%(msg_json,client_address))
                PROTOC.send_msg(connection,msg_json)
            
            elif message[PROTOC.attr.msg_type] == PROTOC.request.check_progs:
                check_progs_inter(SSTAT,PROTOC,message,connection)
            elif message[PROTOC.attr.msg_type] == PROTOC.request.req_output:
                requested_file = message[PROTOC.attr.ab_taskout_path]
                is_exist = check_output_file(SSTAT,requested_file)
                replyJson = PROTOC.gen_req_output_rep(is_exist)
                PROTOC.send_msg(connection,replyJson)
                
                messageJson = PROTOC.recv_msg(connection)
                message = PROTOC.parse(messageJson)

                if message[PROTOC.attr.msg_type] == PROTOC.request.ping:
                    
                    print ('requested file is exist:%r '%is_exist)
                    output_f = open(requested_file,'rb')
                    file_content = output_f.read()
                    output_f.close()
                    PROTOC.send_msg(connection,file_content)
                elif message[PROTOC.attr.msg_type] == PROTOC.request.end:
                    print('requested file is somehow not exist anymore')
                    #PROTOC.gen_req_output_rep(False,None) 
                else:
                    pass
        else:
            empty_message_error()
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
        
    finally:
        connection.close()
