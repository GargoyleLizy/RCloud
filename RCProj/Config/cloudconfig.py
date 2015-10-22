import sys,os,subprocess
import threading
import struct
from collections import namedtuple,deque
import json
import socket
import logging
import zipfile
import time
# **** Helper function ******
# zip files 
# Original version: http://stackoverflow.com/questions/1855095/how-to-create-a-zip-archive-of-a-directory
def zipdir(path, ziph):
    for root,dirs,files in os.walk(path):
        for f in files:
            ziph.write(os.path.join(root,f))


# ***** Important class here

# ***** def_config ******
# This is a class that contains the default configuration and essential labels

class def_config:
    # define the initialization details
    default_port = 12333
    log_delimiter = '#*#'
    # definition of worker log which will be stored in client
    Worker = namedtuple('Worker',
                    ['client_tasks_id','worker_ip','worker_port','key','task_log_idx'
                        ,'sock','prog_st','ab_taskout_path'])
    worker = Worker('client_tasks_id','worker_ip','worker_port','key','task_log_idx'
                        ,'sock','prog_st','ab_taskout_path')

    # define the 
    Bulk_Assign_Dict = namedtuple('Bulk_Assign_Dict',
                    ['ifile','proj','exfun','serverfile','walltime','tasks_id'])
    bulk_assign_dict = Bulk_Assign_Dict(
                    'ifile','proj','exfun','serverfile','walltime','tasks_id')
   
# ***** server_status *****
# This is a class that manipulate the stat of server
# In my client side scripts, There is usually a SSTAT (stands for Server STATus) instance for
# That class.
# It is responsible for dectecting the projects
# responsible for processing the tasks,save the output.
# and maintain the task log of server side.
class server_status:
    # Provide a global status of the server.

    # host_name, port
    host_name = None
    port = None
    # the projs directories locations 
    projs_dir = 'Projs'
    server_dir = 'ServerD'
    config_dir = 'Config'
    TaskOut_dir = 'TaskOut'
    #top_modules = [projs_dir,server_dir,config_dir]
    # absolute path here
    ab_cloud_dir=None
    ab_projs_dir=None
    ab_taskout_dir = None
    projs_list = []

    # define the protocol tags
    Status = namedtuple('Status',['ready','busy','done','exit'])
    status = Status('ready','busy','done','exit')

    TaskLog = namedtuple('TaskLog',['idx','client_tasks_id','tasklist','walltime'])
    tasklogT = TaskLog('idx','client_tasks_id','tasklist','walltime')
    curent_stat=status.ready

    
    def __init__(self):
        self.curent_stat = self.status.ready
    
    # ***** A lot of getter/setter here *****
    # Why need getter/setter? just so I can trace the code quicker.
    def set_ab_cloud_dir(self,path):
        self.ab_cloud_dir = path
        self.taskslog_path = self.ab_cloud_dir+'/'+self.server_dir+'/taskslog'
        self.ab_taskout_dir = self.ab_cloud_dir + '/' + self.TaskOut_dir 
        

    def set_ab_projs_dir(self,path):
        self.ab_projs_dir = path
    
    def set_projs_list(self,avail_list):
        self.projs_list = avail_list

    def set_ip_port(self,ip,port):
        self.host_name = ip
        self.port = port

    def get_curent_status(self):
        return self.curent_stat  
    
    def set_busy(self):
        self.curent_stat = self.status.busy
    def set_ready(self):
        self.curent_stat = self.status.ready

    # ***** Tasks log functions *****
    # taskslog
    taskslog_path = None # was set with set_ab_cloud_dir
    tasks_logs = deque(maxlen=10)

    def check_taskslog(self):
        # retrieve tasks logs from a specified file
        if(self.ab_cloud_dir == None):
            print('absolute path of cloud project is not set yet')
            exit()
        with open(self.taskslog_path,'r') as taskslogfile:
            linenum = 0
            temp_task_log =None
            for line in taskslogfile:
                log_parts = line.split(def_config.log_delimiter)
                if len(log_parts) >= len(self.tasklogT):
                    temp_task_log ={}
                    temp_task_log[self.tasklogT.idx] = int(log_parts[0])
                    temp_task_log[self.tasklogT.client_tasks_id] = log_parts[1]
                    temp_task_log[self.tasklogT.tasklist] = log_parts[2]
                    temp_task_log[self.tasklogT.walltime] = log_parts[3]
                    self.tasks_logs.append(temp_task_log)
                else:
                    print('task log file is corrupted at %d'%linenum)
                linenum+=1
        return self.tasks_logs[-1][self.tasklogT.idx]
    
    # search a specified task in tasklog
    def search_tasklog(self,client_tasks_id,task_log_idx):
        if(self.ab_cloud_dir == None):
            print('absolute path of cloud project is not set yet')
            logging.warning('SSTAT: search_tasklog: absolute path not set yet')
            return False
        else:
            # if found in recent 10 tasklog 
            for task_log in self.tasks_logs:
                if task_log['idx'] == task_log_idx:
                    return True
            # else try the tasklog file
            with open(self.taskslog_path,'r') as tasklogfile:
                linenum = 0
                temp_task_log = None
                for line in tasklogfile:
                    log_parts = line.split(def_config.log.delimiter)
                    if len(log_parts) >= len(self.tasklogT):
                       temp_task_log = {}
                       temp_task_log[self.tasklogT.idx] = int(log_parts[0])
                       temp_task_log[self.tasklogT.client_tasks_id] = log_parts[1]
                       
                       if (temp_task_log[self.tasklogT.idx] == task_log_idx
                        and temp_task_log[self.tasklogT.client_tasks_id] == client_tasks_id):
                           return True
                    else:
                        print('tasklog file is corrupted at %d'%linenum)
                    linenum+=1
            # eventually not found the task in all places
            return False
            


    def add_task_log(self,client_tasks_id,task_list,walltime):
        # add a task_log to the tasks log file
        # and return the log index 
        str_log = str( self.tasks_logs[-1]['idx']+1) + def_config.log_delimiter\
            + client_tasks_id + def_config.log_delimiter \
            + str(task_list ) + def_config.log_delimiter \
            + str(walltime) \
            + '\n'
        tasklog_path = self.ab_cloud_dir+'/'+self.server_dir+'/taskslog'
        
        with open(self.taskslog_path,'a') as taskslogfile:
            taskslogfile.write(str_log)

        temp_task_log ={}
        temp_task_log[self.tasklogT.idx] = self.tasks_logs[-1][self.tasklogT.idx]+1
        temp_task_log[self.tasklogT.client_tasks_id] = client_tasks_id
        temp_task_log[self.tasklogT.tasklist] = task_list
        temp_task_log[self.tasklogT.walltime] = walltime
        self.tasks_logs.append(temp_task_log)
        return temp_task_log['idx']

    # ***** Thread executing the tasks part *****
    current_thread = None
    def execut_task_list(self,task_list,client_tasks_id,task_log_idx,walltime):
        # set stat busy
        #self.curent_stat = self.status.busy 
        # assign task_list to a thread
        print('Debug sstat execut_task_list')
        self.current_thread = task_thread(task_list,self,client_tasks_id,task_log_idx,walltime)
        self.current_thread.start()


# ***** task_thread *****
# This is a helper thread for the server. 
# It is responsible for processing the taskslist and save the result properly
# Currently, Each Server can only hold one thread.
# If there is a need, I sould make the server can hold multiple threads concurrently.
# Probably depend on the number of cores that the server possessed.
class task_thread(threading.Thread):
    # dedicated to solve a task currently owned by the server
    task_list = None
    taskDoneCount = 0
    
    SSTAT = None
    client_tasks_id = None
    task_log_idx = 0
    walltime = None
    def __init__(self,task_list,SSTAT,client_tasks_id,task_log_idx,walltime):
        threading.Thread.__init__(self) 
        self.task_list = task_list
        self.SSTAT = SSTAT
        self.client_tasks_id = client_tasks_id
        self.task_log_idx = task_log_idx 
        self.walltime = walltime
        
        self.proj_path = None
        self.output_dir = None


    def run(self):
        # manipulate the stat of SSTAT between busy and ready
        # processing the tasklist.
        # the output should be handled by the arguments provided by user
        # the standout should be recorded in a specified file in the [Proj_path]/Output/outputlog
        self.SSTAT.set_busy()
        logging.debug('Debug set sstat to %s','busy')

        # this part will clean the Output directory
        self.proj_path = self.task_list[0][protocol.taskL.proj] 
        self.output_dir = self.proj_path + '/Output/'
        os.chdir(self.output_dir)
        #for f in os.listdir(self.output_dir):
        #    os.remove(f)
        # TODO, Need to rewrite to avoid malicious attack
        # Be cautious about the self.output_dir path, I need to rewrite this part later
        for root, dirs, files in os.walk(self.output_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

        stdo_log_path = self.proj_path + '/Output/' + self.client_tasks_id \
                         + def_config.log_delimiter + str( self.task_log_idx )
        time_log_f = open(stdo_log_path,'w')
        # stde_log_path = proj_path + '/Output/' + self.client_tasks_id \
        #                 + def_config.log_delimiter + str( self.task_log_idx ) \
        #                 + '.e'
        # change stdout/err to a file 
        #sys.stderr = open(stde_log_path,'w')
        job_start_time = time.time()
        for task in self.task_list:
            # get essential infomation 
            #itime_log_f = open(stdo_log_path,'w')
            temp_proj = task[protocol.taskL.proj]
            temp_exfun = task[protocol.taskL.exfun]
            temp_task = task[protocol.taskL.task]
            

            logging.debug('Debug self.proj_path: %s',self.proj_path)
            logging.debug('Debug temp_exfun: %s',temp_exfun)
            logging.debug('Debug temp_task: %s',temp_task)
            logging.debug('Debug self.walltime: %s',self.walltime)

            time_log_f.write("%s : start %s\n"
                    % (time.asctime(time.localtime(time.time())),task ) )
            # Directing to the project direcotry
            os.chdir(self.proj_path)
            # executing the task
            try:
                print()
                if(self.walltime == None or self.walltime <= 0):
                    logging.debug('temp_exfun %s; temp_task %s ',temp_exfun,temp_task)
                    # merge them together
                    temp_task = [temp_exfun] + temp_task
                    temp_result = subprocess.check_call(temp_task)
                else:
                    logging.debug('Debug try task with walltime')
                    temp_task = [temp_exfun] + temp_task
                    temp_result = self.subprocess_execute(temp_task
                                                ,self.walltime)
                task[protocol.taskL.ret_code] = temp_result
            except:
                e=sys.exc_info()[0]
                print(e)
                logging.warning('Task %s didnot executed well because %s', temp_task,e)
            time_log_f.write("%s : end\n"% (time.asctime(time.localtime(time.time())) )  )
            
            self.taskDoneCount +=1
        time_log_f.write("total time : %d" % (time.time() - job_start_time))
        time_log_f.flush();
        time_log_f.close();
        # Zip the result and save to TaskOut under RCProj
        zipfile_name = self.SSTAT.ab_taskout_dir+'/' \
                    + str(self.task_log_idx) + def_config.log_delimiter \
                    + self.client_tasks_id + '.zip'
        self.save_zip_output(self.output_dir,zipfile_name)
        logging.info('Server Task: Saved %s output to %s',self.client_tasks_id,zipfile_name)
        
        # set the status back to ready and release the current thread.
        self.SSTAT.set_ready()
        self.SSTAT.current_thread = None
        # restore the stdout/err to normal ones
        #sys.stdout.flush()
        #sys.stdout.close()
        #sys.stderr.flush()
        #sys.stdout = sys.__stdout__
        #sys.stderr = sys.__stderr__
        return self.task_list 

    def get_taskdone_count(self):
        return self.taskDoneCount
    def get_remain_count(self):
        return len(self.task_list) - self.get_taskdone_count()

    def save_zip_output(self,ab_taskout_dir,zipfile_name):
        zipf = zipfile.ZipFile(zipfile_name,'w')
        zipdir(ab_taskout_dir,zipf)
        zipf.close()

    def subprocess_execute(self,command, time_out):
        """executing the command with a watchdog"""

        # launching the command
        c = subprocess.Popen(command)

        # now waiting for the command to complete
        t = 0
        while t < time_out and c.poll() is None:
            time.sleep(1)  # (comment 1)
            t += 1

        # there are two possibilities for the while to have stopped:
        if c.poll() is None:
            # in the case the process did not complete, we kill it
            c.terminate()
            # and fill the return code with some error value
            returncode = -1  # (comment 2)
        else:                 
            # in the case the process completed normally
            returncode = c.poll()
        return returncode 



# ***** protocol *****
# This class provides the protocol between servers and clients
# 1. I use named tuples to unifiy the naming of dict attributes
# 2. client should use gen_**_msg() to generate json request message
# 3. server shoudl use gen_**_rep() to generate corresponding reply message
# 4. It also contains a socket helper function to transfer arbitary length of message
class protocol:
    # define the size of initial messages
    buff_size = 512
    msg_timeout = 3.0
    timeout_retries = 5
    ping_interval = 30
    
    # ***** These namedtuple unified the attribute names used by various 
    #       dicts among the messages transfered bewteen peers *****
    # Attr is the attribute named used in message level
    Attr = namedtuple('Attr',
                ['msg_type','proj','server_stat','ab_projs_dir'
                ,'task_list','walltime'
                ,'client_tasks_id','task_log_idx'
                ,'prog_st','done_remain','is_found_output','is_found_tasklog'
                ,'ab_taskout_path','file_b_content'])
    attr = Attr('msg_type','proj','server_stat','ab_projs_dir'
                ,'task_list','walltime','client_tasks_id','task_log_idx'
                ,'prog_st','done_remain','is_found_output','is_found_tasklog'
                ,'ab_taskout_path','file_b_contnet')
    
    ProgSt = namedtuple('ProgSt',
                ['current_thread_executing','completed','not_found'])
    prog_st = ProgSt('current_thread_executing','completed','not_found')

    # request and reply are the type of messages
    Request=namedtuple('Request',
                ['req_stat','list_proj','req_task','check_progs','req_output'
                    ,'ping','end'])
    request=Request('req_stat','list_proj','req_task','check_progs','req_output'
                    ,'ping','end')
    
    Reply=namedtuple('Reply',
                ['req_stat','list_proj','req_task','check_progs','req_output'])
    reply=Reply('r_req_stat','r_list_proj','r_req_task','r_check_progs','r_req_output')

    # task defines what info should a task include.
    TaskL = namedtuple('TaskL',
                ['proj','exfun','task','sing_idx','ret_code'])
    taskL = TaskL('proj','exfun','task','sing_idx','ret_code')

    # Protocol part
    def parse(self,json_msg):
        message = json.loads(json_msg)
        return message   
   
    # About req_stat
    def gen_req_stat_msg(self,proj):
        message = {}
        message[self.attr.msg_type] = self.request.req_stat
        message[self.attr.proj] = proj
        return json.dumps(message)

    def gen_req_stat_rep(self,server_status,proj_is_avail,ab_projs_dir):
        message = {}
        message[self.attr.msg_type] = self.reply.req_stat
        message[self.attr.server_stat] = server_status
        message[self.attr.proj] = proj_is_avail
        message[self.attr.ab_projs_dir] = ab_projs_dir
        return json.dumps(message)

    # About req_task
    def gen_req_task_msg(self,client_tasks_id,task_list,walltime):
        message = {}
        message[self.attr.msg_type] = self.request.req_task
        message[self.attr.client_tasks_id] = client_tasks_id
        # convert list of tuple to list of dict
        # the task_list contains a list of tuples, each tuple contain a task
        # with proj_path,execute function, argumtns/task, and walltime.
        # The task_list are generated by taskmaster.
        task_dict_list = []
        for task in task_list:
            task_dict = {}
            task_dict[self.taskL.proj] = task[0]
            task_dict[self.taskL.exfun] = task[1]
            task_dict[self.taskL.task] = task[2]  
            task_dict[self.taskL.sing_idx] = task[3]            
            task_dict_list.append(task_dict)
        message[self.attr.task_list] = task_dict_list
        message[self.attr.walltime] = walltime
        return json.dumps(message)

    def gen_req_task_rep(self,client_tasks_id,log_idx):
        message={}
        message[self.attr.msg_type] = self.reply.req_task
        message[self.attr.client_tasks_id] = client_tasks_id
        message[self.attr.task_log_idx] = log_idx
        return json.dumps(message)

    # about check_prog
    def gen_check_prog_msg(self,client_tasks_id,task_log_idx):
        message = {}
        message[self.attr.msg_type] = self.request.check_progs
        message[self.attr.client_tasks_id] = client_tasks_id
        message[self.attr.task_log_idx] = task_log_idx
        return json.dumps(message)
   
    def gen_ping_msg(self):
        # If the task is currently processing, keeps pinging server
        message = {}
        message[self.attr.msg_type] = self.request.ping
        return json.dumps(message)
    
    def gen_end_msg(self):
        message = {}
        message[self.attr.msg_type] = self.request.end
        return json.dumps(message)
    # used when the task is currently processing
    def gen_check_prog_curentT_rep(self,done_count,remain_count):
        # if task is currently processing, return the progress
        message = {}
        message[self.attr.msg_type] = self.reply.check_progs
        message[self.attr.prog_st] = self.prog_st.current_thread_executing
        message[self.attr.done_remain] = [done_count,remain_count]
        return json.dumps(message)
    # used when the task is processed in past result
    def gen_check_prog_Out_rep(self,is_found_Output,is_found_tasklog,ab_taskout_path):
        # if task is not currently processing, search in Output/tasklog and return result
        message = {}
        message[self.attr.msg_type] = self.reply.check_progs
        message[self.attr.is_found_output] = is_found_Output
        message[self.attr.is_found_tasklog] = is_found_tasklog
        if is_found_Output == True:
            message[self.attr.prog_st] = self.prog_st.completed
            message[self.attr.ab_taskout_path] = ab_taskout_path
        else:
            message[self.attr.prog_st] = self.prog_st.not_found
        return json.dumps(message)
        

    def gen_req_output_msg(self,ab_taskout_path):
        message = {}
        message[self.attr.msg_type] = self.request.req_output
        message[self.attr.ab_taskout_path] = ab_taskout_path
        return json.dumps(message)

    def gen_req_output_rep(self,is_found_output):
        message = {}
        message[self.attr.msg_type] = self.reply.req_output
        message[self.attr.is_found_output] = is_found_output
        return json.dumps(message)
    # socket helper function: allows transferring arbitary length of message. 
    # Get it from 
    # http://stackoverflow.com/questions/17667903/python-socket-receive-large-amount-of-data
    def send_msg(self,sock, msg):
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        print('PROTOC send:%s'%msg)
        sock.sendall(msg)

    def recv_msg(self,sock):
        # Read message length and unpack it into an integer
        raw_msglen = self.recvall(sock, 4)
        if not raw_msglen:
            print('not raw')
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # Read the message data
        message = self.recvall(sock,msglen)
        print('PROTOC receive:%s'%message)
        return message

    def recvall(self,sock, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = ''
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                print('not packet')
                return None
            data += packet
        return data


