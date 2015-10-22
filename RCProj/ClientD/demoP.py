import argparse

import checkIP
import demoBulkAssign
import checkProgs
import getOutput 

from ..Config.taskmaster import taskmaster
from ..Config.cloudconfig import *
# ***** Handle input/test part *****
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--ifile')
    parser.add_argument('-p','--proj')
    parser.add_argument('-ef','--exfun')
    parser.add_argument('-sf','--serverfile')
    parser.add_argument('-wt','--walltime')
    parser.add_argument('-id','--tasks_id')
    args=parser.parse_args()

    print('# # # # # This is a demo script for How to use the web server/client # # # # #')
    print('# #Introduction: ')
    print('This server/client program is aimed to provide an user friendly interface of Cloud '
            +' resources for scientific users.')
    tip = raw_input("\npress any key to continue;\n")
    #print('In server side, each Project will has a directory and Users should design their  '
    #        +'program so that the program will put all the output to a /Output directory.')
    print('First, Let us check the availability of servers by using checkIP.py.')
    print('It will take into a server file, which will record the essential information of the servers. ')
    tip = raw_input("\npress any key to continue;\n")

    print('The format in the server_file should be: ')
    print('\n IP address '+ def_config.log_delimiter+ ' Port if not default '+ def_config.log_delimiter+' private Key if any \n')
    tip = raw_input("\npress any key to continue;\n")

    print('As an example, this is the conetent of serverfile used by demo:')
    #print('args.serverfile is '%( str(args.serverfile ) ))
    print 'args.serverfile is '+ args.serverfile + ' :'
    with open(args.serverfile,'r') as serverF:
        for line in serverF:
            print line
    print('The \'None\' means we will use default value.')
    tip = raw_input("\npress any key to continue;\n")
    
    print('Ready to run \n Workers = checkIP.fetch_workers(serverfile) ' 
            +'\n checkIP.check_servers(Workers,\'test\')' )
    print('\nIf you finished the content above and ready to run  '
            + 'the checkIP.py.')
    tip = raw_input("\npress any key to continue;\n")

    print('=====================checkIP.py=================================')
    Workers = checkIP.fetch_workers(args.serverfile)
    Workers = checkIP.check_servers(Workers,'test')
    print('=====================checkIP.py=================================')
    tip = raw_input("\npress any key to continue;\n")

    print('As can be seen, check_servers take two arguments, first one is servers info, second'
            + ' one is the name of project, \'test\' is a fault project that used for test')

    print('\nOK, we got the available workers now. If you didnot get any available workers,'
            +'please check if your local network blocked some ports.')
    print('Now we can assign tasks for them.\n'
            +'In this demo, we will run a demo.py in server side to process serval tasks.')
    print('demo.py will take two arguments, one is number of seconds to elapse. second one is'
            +' the location of /Output directory\n')
    print('demo.py -t runningtime -o output_directory')
    print('First, put all the tasks in a file, in this demo, tasklist is '+ args.ifile+':')
    with open(args.ifile,'r') as ifile:
        for line in ifile:
            print(line)
    tip = raw_input("\npress any key to continue;\n")
    
    print('Second, you will need a taskmaster class to handle the tasks.'
            +'\nCheck demoBulkAssign.py to get more information')
    tip = raw_input("\npress any key to continue;\n")
    print('Third, specify which project will be used in this job. In this demo, the project is '
            +args.proj)
    tip = raw_input("\npress any key to continue;\n")

    print('Forth, specify which external function wil be called, '
            + 'in this demo, executing function is '+ args.exfun)
    tip = raw_input("\npress any key to continue;\n")

    print('Fifth, specify wall time for processing a single task (If you want). In this demo, '
            +' wall time for individual task is '+ str(args.walltime) )
    tip = raw_input("\npress any key to continue;\n")

    print('Last, specify an ID for your job. \nAnd please do not use the same ID '
            +'to submit the job repeatedly. Because the later job will write over your '
            + 'previous ones.\n'
            +'In this demo, tasks ID is '+ args.tasks_id)
    tip = raw_input("\npress any key to continue;\n")

    print('================== demo Assign tasks ====================')
    # check input 
    if(args.ifile == None or args.proj == None or args.exfun == None or args.tasks_id==None):
        print('Essential information missing.')
        print('-id tasks_id -i inputfile -p project -ef external function -id tasks_id')
        exit()
    



    # user need to rewrite the taskmaster
    # Task master is responsible for extracting tasks from input file specified by user
    # and producing arguments provided as task to servers:
    # based on information returned by server (absolut proj path/output path in server)
    # It also decides which server get which tasks (That is why it is called taskmaster)

    # Example: demo.py is a python file need input as 
    # -o output_dir, a directory to output result
    # -t counting time, an argument decide how many ticks the program counts
    class demo_task_master(taskmaster):
        
        # for each line in inputfile, extract the information
        def append_task(self,task_str):
            # just for demo, task_parts = task_str.split( whaterver you decided)
            temp_task = {}
            temp_task['counting_time'] = int(task_str)
            if temp_task['counting_time'] <= 0 :
                pass
            else:
                self.task_list.append(temp_task)

        def get_idx_task(self,task_idx):
            # constant string priori part
            if(task_idx < len(self.task_list)):
                temp_task = self.task_list[task_idx]
                temp_arguments = []
                temp_arguments.append('demo.py')
                # every program should output all their outputs to a /Output directory
                # They can either use -o or merge this to their own arguments
                temp_arguments.append('-o'+self.output_dir)
                
                temp_arguments.append('-t' +str(temp_task['counting_time']))
                #temp_arguments = 'demo.py ' \
                #            + ' -o ' + self.output_dir \
                #            + ' -t ' + str( temp_task['counting_time'] )
                return temp_arguments
            else:
                return None


    # put all the input arguments to a dict
    inputs_dict={}
    inputs_dict[def_config.bulk_assign_dict.ifile] = args.ifile
    inputs_dict[def_config.bulk_assign_dict.proj] = args.proj
    inputs_dict[def_config.bulk_assign_dict.exfun] = args.exfun
    inputs_dict[def_config.bulk_assign_dict.serverfile] = args.serverfile
    inputs_dict[def_config.bulk_assign_dict.walltime] = args.walltime
    inputs_dict[def_config.bulk_assign_dict.tasks_id] = args.tasks_id
    
    Workers = demoBulkAssign.assign_tasks_bulk(inputs_dict,demo_task_master)
    print Workers
    print('=================== demo assign tasks ======================')
    tip = raw_input("\npress any key to continue;\n")


    print('After Assign tasks, the tasks information would be stored in a [TasksID].record '
            +'file which would locate in the same directory of user\'s input file.')
    print('In this demo, the content of '+args.tasks_id+'.record is:')
    parent_dir = os.path.abspath(os.path.join(args.ifile,os.pardir))
    #print('parent dir path of input file: %s'%p_dir)
    task_record_file = parent_dir + '/'+args.tasks_id +'.record'
    with open(task_record_file,'r') as recordfile:
        for line in recordfile:
            print line

    tip = raw_input("\npress any key to continue;\n")

    print('Now,users can start keep checking the progress of their tasks with checkProgs.py')
    print('When all the tasks are processed, users can press \'Enter\' to break the checking'
            +' loop and get the tasks Output in zip format')
    print('During checkProgs.py, users could break the checking loop by pressing \'Enter\' '
            +'and use the record file to check the progress later.')
    
    print('Press any key to start checkProgs.check_task_progs(Workers,PROTOC.ping_interval):')
    

    print('=================== start check progs ======================')
    Workers = checkProgs.check_task_progs(Workers,protocol.ping_interval)
    print('================== check progs stoped ======================')
    tip = raw_input("\npress any key to continue;\n")

    print('=================== start downloading the output ===========')
    getOutput.get_output_socket(Workers,'./')
    print('=================== downloading finished ===================')
    print('If checkProgs finished by itself, you should find the output zip file in the same directory'
            + ' of the job recordfile.')
    print('If output checkProgs is exited by user\'s input. Just try \"sh checkProgs.sh\" ' )
    print('And if every thing goes right, you will find the output in '+parent_dir)


