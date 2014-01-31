Input:
	Given a file, consisting of lines:
		student 1 George
		mark EXC 1 70
		student 2 Anna
		mark ADBS 1 80
		mark EXC 2 65
		mark TTS 1 80
	Where line starting with "student" is followed by student id and name,
	whereas one starting with "mark" is followed by course abbreviation, student
	id and course mark.
Output:
	Join of these two schemas on the student id parameter in a following form:
		name --> (course1, mark1) (course2, mark2) (course3, mark3) . . .

Run:
	hadoop jar ${HADOOP_INSTALL}/contrib/streaming/hadoop-0.20.2-streaming.jar \
	-input /user/s1250553/ex2/uniSmall.txt -output \
	/user/s1367762/tests/join-test-cat -mapper mapper.py -reducer reducer.py  -file \
	./mapper.py -file ./reducer.py -jobconf stream.map.output.field.separator=_ \
	-jobconf  stream.num.map.output.key.fields=1
