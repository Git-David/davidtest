#!/usr/bin/python

import os, sys

# os.system('say "Hi David"')
temp_namedpipe = sys.argv[1]
file = open("./result_test.json","w") 
with open(temp_namedpipe, 'r') as read_fifo:
    while True:
        filecontent = read_fifo.read()
        if len(filecontent) == 0:
                os.remove(temp_namedpipe)
                break
        else:
            file.write(filecontent) 
            # os.system('say "{0}"'.format(line))



# os.system('say "byebye"')
file.close() 
 

