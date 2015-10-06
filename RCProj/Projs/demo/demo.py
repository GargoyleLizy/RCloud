import argparse
import time

# default values
waiting_time = 5
#output_dir = "./output/"


# testFun take one actual argument 
#   -t : for counting time
#   -o : for output directory
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-o','--odir')
    parser.add_argument('-t','--extime')
    args=parser.parse_args()


if(args.odir == None or args.extime == None):
    print('Essential input is missing, required output_dir and execution time')
    exit()
else:
    waiting_time = float(args.extime)
    output_dir = args.odir


start_time = time.time()
mid_time = start_time
output_file = open(output_dir + '/demoOutFile'+str(waiting_time),'w+')

while True:
    cur_time = time.time()  
    if cur_time - start_time > waiting_time:
        break
    else:
        if cur_time - mid_time > 1:
            output_file.write('%s : tick %d \n' % (time.asctime(time.localtime(time.time())), 
                cur_time - start_time )   )
            mid_time = cur_time

print("finished task with %f sec time."%waiting_time)

