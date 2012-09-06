# compare the output of programs automatically
# need to add downloads from HDFS

import os
import shlex
import subprocess
l = ["v%s v%sw" %(x,x) for x in xrange(6)]
list = ["v0", "v0w", "v5w"]
l3 = []
for x in l:
        xx = x.split(" ")
        l3.append(xx[0])
        l3.append(xx[1])
l2 = ["output_"+x for x in l3]
i = 0
for x in xrange(len(l2)-1):
	print l2[x]+" "+l2[x+1]
        
	process = subprocess.Popen(shlex.split("""diff -s %s %s""" %(l2[x], l2[x+1])), stdout=subprocess.PIPE)
	process.wait()
	print (process.returncode)


