import sys
import subprocess

sys.stdout = open('./olog','w')
exfun = 'echo'
subprocess.check_call([exfun,'really?'])

print "fack the std"



