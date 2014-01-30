package org.krzysztow.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;

import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.*;

//for the writable comparable class
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;

public class TransposeMatrix extends Configured implements Tool {
	private static String MATRIX_COLS_NO = "exc.matrix.cols.no";

	public static class CoordinatesPartitioner implements Partitioner<Coordinates, Text> {
		private int newRowsCount = 0;
				
		@Override
		public void configure(JobConf job) {
			newRowsCount = job.getInt(MATRIX_COLS_NO, 0);
		}
		
		@Override 
		public int getPartition(Coordinates key, Text value, int numPartititons) {
			return (int)(1.0 * key.getFirst() / newRowsCount * numPartititons);
			//return 1;//key.getFirst() % numPartititons;
		}
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Coordinates, Text> {
		private int MatrixColsNo = 0;
		private Integer currentRow;
		private Coordinates reusePos = new Coordinates();
		private Text reuseValueText = new Text();

		@Override 
		public void configure(JobConf config) {
			super.configure(config);
			MatrixColsNo = config.getInt(MATRIX_COLS_NO, 0);
		}

		public void map(LongWritable key, Text value, OutputCollector<Coordinates, Text> output, Reporter reporter) throws IOException {
			String rowArray[] = value.toString().split("\\s+");

			currentRow = Integer.parseInt(rowArray[0]);

			for (int i = 1; i < rowArray.length; ++i) {
				reusePos.set(i - 1, currentRow);
				reuseValueText.set(rowArray[(int)i]);
				output.collect(reusePos, reuseValueText);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Coordinates, Text, IntWritable, Text> {
		private static IntWritable reuseRowId = new IntWritable();
		private static Text reuseRowValue = new Text();
		private static StringBuilder rowBuilder = new StringBuilder();

		@Override 
		public void configure(JobConf config) {
			super.configure(config);
		}

		public void reduce(Coordinates key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			reuseRowId.set(key.getFirst());
			while (values.hasNext()) {
				rowBuilder.append(values.next().toString());
				if (values.hasNext())
					rowBuilder.append(" ");
			}
			//it could be optimized here - getBytes insted of toString() conversion and then back to Text
			reuseRowValue.set(rowBuilder.toString());
			output.collect(reuseRowId, reuseRowValue);
			//clear the builder, so that it could be used in another reduce() invocation
			rowBuilder.setLength(0);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TransposeMatrix(), args);
		System.exit(exitCode);
	}

	String getFileLine(String fileNamePath, Configuration conf) {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			System.err.println("Filesystem error");
			return "";
		}

		Path path = new Path(fileNamePath);		

		boolean fileExists = false;
		try {
			fileExists = fs.exists(path);
		} catch (IOException e) {
			fileExists = false;
		}

		if (!fileExists) {
			//file doesn't exist, return
			System.err.println("The given file (" + path + ") doesn't exist (fs: " + fs + ")");
			return "";
		}

		FSDataInputStream in = null;
		InputStreamReader streamReader = null;
		BufferedReader buffReader = null;
		try {
			in = fs.open(path);
			streamReader = new InputStreamReader(in);
			buffReader = new BufferedReader(streamReader);
			return buffReader.readLine();
		} catch (IOException e) {
			try {
				if (null != in)
					in.close();
				if (null != streamReader)
					streamReader.close();
				if (null != buffReader)
					buffReader.close();
			} catch (IOException e2) {}


			return "";
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (2 != args.length) {
			System.err.println("File path not provided");
			return 1;
		}


		//[find-cols-no] before starring the MapReduce job, we get know how many colums are there
		//to do so - fetch the first line of the file and count numbers (except the first row-no)
		String firstLine = getFileLine(args[0], conf);
		if ("" == firstLine) {
			System.err.println("Couldn't read from the file its first line");
			return 3;
		}

		System.out.println("First line: " + firstLine);
		//the contract is as follows:
		// -first number is a line number
		// -all the other numbers are matrix values
		String rowArray[] = firstLine.split("\\s+");
		Integer colsNo = (rowArray.length - 1);
		System.out.println("Cols no is " + colsNo);
		//END [find-cols-no]


		JobConf jobConf = new JobConf(TransposeMatrix.class);
		jobConf.setLong(MATRIX_COLS_NO, colsNo);
		jobConf.setJobName("simple");

		System.out.println("Number of reducers set to 5");
		jobConf.setNumReduceTasks(5);

		jobConf.setMapOutputKeyClass(Coordinates.class);
		jobConf.setMapOutputValueClass(Text.class);

		jobConf.setOutputKeyClass(Integer.class);
		jobConf.setOutputValueClass(Text.class);

		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);

		jobConf.setPartitionerClass(CoordinatesPartitioner.class);
		jobConf.setOutputValueGroupingComparator(CoordinatesGrouper.class);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		JobClient.runJob(jobConf);

		return 0;
	}
}
