CC = g++
#PLATFORM = Linux-amd64-64
PLATFORM = Linux-i386-32
CPPFLAGS = -m32 -I$(HADOOP_INSTALL)/c++/$(PLATFORM)/include 
HDFS = hadoop dfs

PROGNAME = toUpper 
CFILES = toupper.cpp

#INPUTDIR = $(MYPATH)/$(PROGNAME)-input/
#INPUTDIR = /user/s1250553/ex2/webSmall.txt
INPUTDIR = /user/s1250553/ex2/webLarge.txt
OUTDIR = $(MYPATH)/$(PROGNAME)-output/

#linked against static library
HPIPES_LIB = $(HADOOP_INSTALL)/c++/$(PLATFORM)/lib/libhadooppipes.a

#modified - static instead dynamic library added
$(PROGNAME): $(CFILES) 
	$(CC) $(CPPFLAGS) $< -Wall $(HPIPES_LIB) -L$(HADOOP_INSTALL)/c++/$(PLATFORM)/lib -lhadooputils -lpthread -g -O2 -o $@

#create bin dir in my home directory on hdfs
prepare:
	-$(HDFS) -mkdir $(MYPATH)/bin/
	-$(HDFS) -rm $(MYPATH)/bin/$(PROGNAME)
	-$(HDFS) -put ./$(PROGNAME) $(MYPATH)/bin/
	-$(HDFS) -rmr $(OUTDIR)

clean:
	rm $(PROGNAME)
	-$(HDFS) -rm $(MYPATH)/bin/$(PROGNAME)
	-$(HDFS) -rmr $(OUTDIR)

#1) delete existing program, 2) upload the new one, 3) run it
run: prepare
	hadoop pipes -conf ./config.xml -input $(INPUTDIR) -output $(OUTDIR)

runLocal:
	hadoop pipes -conf ./local-conf.xml -conf ./config.xml -input file:///$(PWD)/input/ -output file:///$(PWD)/output

