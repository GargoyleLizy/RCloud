# ------ Task_Master ------ 
# A class responsible for fetching and distributing tasks 
# output_dir: All output of tasks would goto there.
# input_file_path: The input tasks should be put there
# project_path : The path of essential resources for matlab function 
class taskmaster:
    input_file_path=""
    proj = ""

    project_path=""
    output_dir=""
    
    # stores tasks
    task_list=[]
    
    # Used to record tasks for better workload balancing
    task_current_indx=0

    # Used for simple workload distribution
    task_chunks=[]
    
    def __init__(self,input_file_path,project):
        self.input_file_path=input_file_path
        self.project=project

    def set_input(self,input_file):
        self.input_file_path=input_file
    
    #---Functions extract tasks from inputfile---
    # This function assume each line represent a task
    # Modify it if this assumption does not fit your project.
    def extract_tasks(self):
        with open(self.input_file_path,'r') as infile:
            for line in infile:
                self.append_task(line)

    #--- !!! Modify append_task() to fit different projects
    def append_task(self,task_str):
        task_parts = task_str.split('-')
        if(len(task_parts)>=6):
            #temp = self.Task(task_parts[0],task_parts[1],task_parts[2],task_parts[3],task_parts[4],int(task_parts[5]),"undone",0)
            if(task_parts[1]=='BAFBA'):
                temp = {'model':task_parts[0], 'task_fun':task_parts[1],'rxnlist':task_parts[2]
                        ,'targetRxn':task_parts[3],'substrateRxn':task_parts[4]
                        ,'MaxKOs':int(task_parts[5]),'imax':0
                        ,'status':1,'time':0,'worker':-1}
                self.task_list.append(temp)
            elif(task_parts[1]=='BHFBA' or task_parts[1]=='DBFBA'):
                temp = {'model':task_parts[0], 'task_fun':task_parts[1],'rxnlist':task_parts[2]
                        ,'targetRxn':task_parts[3],'substrateRxn':task_parts[4]
                        ,'MaxKOs':int(task_parts[5]),'imax':int(task_parts[6])
                        ,'status':1,'time':0,'worker':-1}
                self.task_list.append(temp)
        else:
            print("task is not correct formated:",task_str)
    
    # --- !!! Modify pop_task() to fit different projects 
    # pop the current undone task and cat them together.
    def pop_task(self):
        if(self.remain_task()):
            temp_task = self.task_list[self.task_current_indx]
            self.task_current_indx+=1
            matlab_argums_cont = "\"math_task("+ "\'"+self.project_path+"\'" \
                        +",\'"+ self.output_dir + "\'" \
                        +",\'"+ temp_task['model'] + "\'" \
                        +",\'"+ temp_task['task_fun'] +"\'" \
                        +",\'"+ temp_task['rxnlist'] + "\'" \
                        +",\'"+temp_task['targetRxn'] + "\'" \
                        +",\'" + temp_task['substrateRxn']+"\'" \
                        +"," + str(temp_task['MaxKOs']) \
                        +"," + str(temp_task['imax']) \
                        + ");exit\""

            return (self.task_current_indx-1,self.matlab_argums_pre + matlab_argums_cont)
        else: 
            return None
    
    # Convert an external appropriate "task" directory to string argument for subp
    #def convert(self):



    # --- Check remaining task number 
    def remain_task(self):
        return len( self.task_list ) > self.task_current_indx
    
    def select_task(self,idx):
        if(idx <len(self.task_lsit)):
            return self.task_list[idx]
        else:
            return None

    ###### Distribute the tasks as blocks #####
    # This is a not so intelligent way to distribute the tasks
    def chunk_tasks(self,num_worker):
        self.task_chunks=[]
        avg = int( len(self.task_list)/num_worker)
        last=0
        for idx in range(num_worker):
            last+=avg
            self.task_chunks.append(last)
        self.task_chunks[num_worker-1]=len(self.task_list)
   
    def get_idx_task(self,task_idx):
        # constant string pre
        matlab_argums_pre = "-nodesktop -nodisplay -nosplash -r "
        if(task_idx<len(self.task_list)):
            temp_task = self.task_list[task_idx]
            matlab_argums_cont = "\"math_task("+ "\'"+self.project_path+"\'" \
                        +",\'"+ self.output_dir + "\'" \
                        +",\'"+ temp_task['model'] + "\'" \
                        +",\'"+ temp_task['task_fun'] +"\'" \
                        +",\'"+ temp_task['rxnlist'] + "\'" \
                        +",\'"+temp_task['targetRxn'] + "\'" \
                        +",\'" + temp_task['substrateRxn']+"\'" \
                        +"," + str(temp_task['MaxKOs']) \
                        +"," + str(temp_task['imax']) + ");exit\""
            return [matlab_argums_pre + matlab_argums_cont ]
        else: 
            return None

    
    # Execute the tasks with rank as input
    def execute_taskchunks(self,worker_idx,time_out):
        node_task_list=[]
        if(worker_idx==0):
            pre_idx=0
        else:
            pre_idx=self.task_chunks[worker_idx-1]
        
        for idx in range(pre_idx,self.task_chunks[worker_idx]):
            temp_start_time = time.time()
            
            if(time_out==None):
                temp_result=subprocess.check_call([ExtnFunction,self.get_idx_task(idx)])
            else:
                temp_result = subprocess_execute([ExtnFunction,self.get_idx_task(idx)],time_out)

            temp_execution_time = time.time()-temp_start_time
            self.task_list[idx]['time']=temp_execution_time
            self.task_list[idx]['status']=temp_result
            self.task_list[idx]['worker']=worker_idx
            node_task_list.append( self.task_list[idx]  )
       
        return node_task_list

    def get_idx_chunk(self,worker_idx,ef):
        node_task_list=[]
        if(worker_idx==0):
            pre_idx=0
        else:
            pre_idx=self.task_chunks[worker_idx-1]
        
        for idx in range(pre_idx,self.task_chunks[worker_idx]):
            temp_task = (self.project_path,ef, self.get_idx_task(idx),idx)
            node_task_list.append(temp_task)
            self.task_list[idx]['worker']=worker_idx
        
        return node_task_list



    ##### END Distribute the tasks as blocks END #### ##### #####

   
    # --- Return total number of tasks
    def total_num_task(self):
        return len(self.task_list)

