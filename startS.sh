#!/bin/bash
# change working directory to local directory
cd "$(dirname "$0")"
# call the server at the root directory out of the project 
# and call them as package
python -m RCProj.ServerD.serverS -cd ./RCProj -t test 
