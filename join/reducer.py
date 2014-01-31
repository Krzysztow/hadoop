#!/usr/bin/python

import sys;

'''
	Output of the mapper has form:
		1_student	George
		1_mark	(EXC, 70)
		2_student	Anna
		1_mark	(ADBS, 80)

	Note: for grouping I use: -jobconf stream.map.output.field.separator=_ -jobconf  stream.num.map.output.key.fields=1, so
	after shuffling it becomes:
		942	student	Amison
		951	student	Alderson
		960	student	Ainsley
		979	student	Acomb
		988	student	Abby
		997	student	Abby
		997	mark	(BIO1, 91)

	Output has to have a form of:
		George --> (ADBS, 80) (EXC, 70) (TTS, 80)
		Anna --> (EXC, 65) 
'''
def printStudent(studentName, coursesList):
	print "{0} --> {1}".format(studName, " ".join(marksList));

currentId = "";
studName = "Unknown";
marksList = [];

'''For debugging read sorted mapper output saved to file'''
#f = open("./sorted-mapper-output.txt", "r");
#f = open(sys.argv[1], "r");
#for line in f:
for line in sys.stdin:
	line = line.strip();
	parts = line.split("\t", 2);
	key = parts[0];

	if (key != currentId):	
		'''We process new student'''
		if ("" != currentId):	
			'''There was a previous student'''
			printStudent(studName, marksList);
		currentId = key;
		studName = "";
		marksList = [];
	
	if ("mark" == parts[1]):
		marksList.append(parts[2]);
	elif ("student" == parts[1]):
		studName = parts[2];

'''We are done, but haven't flushed the last student'''
if ("" != currentId):
	printStudent(studName, marksList);

