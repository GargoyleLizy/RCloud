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


    if(args.ifile == None or args.proj == None or args.exfun == None or args.tasks_id==None):
        print('Essential information missing.')
        print('-id tasks_id -i inputfile -p project -ef external function -id tasks_id')
        exit()

        # put all the input arguments to a dict
    inputs_dict={}
    inputs_dict[def_config.bulk_assign_dict.ifile] = args.ifile
    inputs_dict[def_config.bulk_assign_dict.proj] = args.proj
    inputs_dict[def_config.bulk_assign_dict.exfun] = args.exfun
    inputs_dict[def_config.bulk_assign_dict.serverfile] = args.serverfile
    inputs_dict[def_config.bulk_assign_dict.walltime] = args.walltime
    inputs_dict[def_config.bulk_assign_dict.tasks_id] = args.tasks_id
    
    Workers = demoBulkAssign.assign_tasks_bulk(inputs_dict,taskmaster)
    print Workers
    Workers = checkProgs.check_task_progs(Workers,protocol.ping_interval)
    getOutput.get_output_socket(Workers,'./')
