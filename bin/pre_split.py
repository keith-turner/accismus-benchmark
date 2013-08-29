#!/usr/bin/python

import os
import sys
import subprocess

if len(sys.argv) != 4 :
  print "Usage : "+sys.argv[0]+" <user> <pass> <table>"
  sys.exit(1)


s=""
for x in range(0,4):
  for y in range(0,16):
    s += 'url:%x%x ' % (x,y)

subprocess.call([os.environ['ACCUMULO_HOME']+'/bin/accumulo','shell','-u',sys.argv[1],'-p',sys.argv[2],'-e','addsplits -t '+sys.argv[3]+' '+s]);
