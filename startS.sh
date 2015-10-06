#!/bin/bash
cd "$(dirname "$0")"
#cd /home/ubuntu/RCloud/
# call the server at the root directory out of the project 
# and call them as package
python -m RCProj.ServerD.echoS -cd ./RCProj 
