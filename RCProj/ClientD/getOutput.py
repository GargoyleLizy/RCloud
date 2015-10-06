import subprocess
import socket
import os
import logging

from ..Config.cloudconfig import def_config
from ..Config.cloudconfig import protocol

PROTOC = protocol()

def save_output2file(output_dir, fileName, file_b_content):
    file_path = os.path.join(output_dir,fileName)
    savef = open(file_path,'wb')
    savef.write(file_b_content)
    savef.close


def get_output_scp(workers):
    Exfun = 'scp'
    for worker in workers:
        if worker[def_config.worker.prog_st] == PROTOC.prog_st.completed:
            print worker[def_config.worker.ab_taskout_path] 

def get_output_socket(workers,local_output_dir):
    for worker in workers:
        if worker[def_config.worker.prog_st] == PROTOC.prog_st.completed:
            temp_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            server_address = (worker[def_config.worker.worker_ip]
                    ,worker[def_config.worker.worker_port])
            worker[def_config.worker.sock] = temp_sock
             
            try:
                worker[def_config.worker.sock].connect(server_address)

                messageJson = PROTOC.gen_req_output_msg(worker[def_config.worker.ab_taskout_path])
                # ab taskout path is put to worker by checkProg.py
                PROTOC.send_msg(worker[def_config.worker.sock],messageJson)

                replyJson = PROTOC.recv_msg(worker[def_config.worker.sock])
                reply = PROTOC.parse(replyJson) 
                if reply[PROTOC.attr.is_found_output]:
                    messageJson = PROTOC.gen_ping_msg()
                    PROTOC.send_msg(worker[def_config.worker.sock],messageJson)
                    reply_Bfile = PROTOC.recv_msg(worker[def_config.worker.sock])
                    
                    fileName = worker[def_config.worker.client_tasks_id] \
                            + def_config.log_delimiter \
                            + worker[def_config.worker.worker_ip]+ '-' \
                            + str(worker[def_config.worker.task_log_idx] ) + '.zip'
                    save_output2file(local_output_dir,fileName,reply_Bfile)

                    print('saved success')
                    logging.info('Save output of %s from %s to %s'
                            ,worker[def_config.worker.client_tasks_id]
                            ,worker[def_config.worker.worker_ip]
                            ,fileName)
                else:
                    messageJson = PROTOC.gen_end_msg()
                    PROTOC.send_msg(worker[def_config.worker.sock],messageJson)
                    print (' requested file is missing in server side')
            finally:
                worker[def_config.worker.sock].close()


#if __name__ == "__main__":

