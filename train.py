#!/usr/bin/python

import os, sys

import time
import tensorflow as tf

# os.system('say "Hi David"')
temp_namedpipe = sys.argv[1]
# file = open("./result_test.json","w") 
# with open(temp_namedpipe, 'r') as read_fifo:
#     while True:
#         filecontent = read_fifo.read()
#         if len(filecontent) == 0:
#                 os.remove(temp_namedpipe)
#                 break
#         else:
#             file.write(filecontent) 
#             # os.system('say "{0}"'.format(line))



# # os.system('say "byebye"')
# file.close() 

# time.sleep(35)


def get_pipeline_data(path):
    """
        Get pipeline data.

        Args:
            path: Path of pipeline.
        Retunr:
            Data string.
    """
    with open(path, 'r') as read_fifo:
        while True:
            filecontent = read_fifo.read()
            if len(filecontent) == 0:
                    # os.remove(path)
                    break
            else:
                os.remove(path)
                return filecontent

 


config = get_pipeline_data(temp_namedpipe)

file = open("./hahahhhaahah.json","w") 
file.write(config) 
file.close()