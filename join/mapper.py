#!/usr/bin/python

import sys;

for line in sys.stdin:
	values = line.split();
	
	'''
	different schema rows look like follows: 
		student 1 George
		mark EXC 1 70
	'''

	if ("mark" == values[0]):
		print "{0}_{1}\t({2}, {3})".format(values[2], values[0], values[1], values[3]);	
	elif("student" == values[0]):
		print "{0}_{1}\t{2}".format(values[1], values[0], values[2]);
	
