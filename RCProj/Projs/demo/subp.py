import subprocess
import time
import os,zipfile

def subprocess_execute(command, time_out):
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


def zipdir(path, ziph):
    for root,dirs,files in os.walk(path):
        for f in files:
            ziph.write(os.path.join(root,f))

if __name__ == '__main__':
    subprocess_execute(['python', 'demo.py', '-o./Output','-t2' ], 5)
    zipf=zipfile.ZipFile('../demo.zip','w')
    zipdir('./Output',zipf)
    zipf.close()



