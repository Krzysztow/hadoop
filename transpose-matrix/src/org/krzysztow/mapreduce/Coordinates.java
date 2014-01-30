package org.krzysztow.mapreduce;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.*;

public class Coordinates implements WritableComparable<Coordinates> {
		private int first;
		private int second;

		public Coordinates() {
			set(0, 0);
		}

		public Coordinates(int first, int second) {
			set(first, second);
		}

		public void set(int first, int second) {
			this.first = first;
			this.second = second;
		}

		public int getFirst() {
			return this.first;
		}

		public int getSecond() {
			return this.second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readInt();
			second = in.readInt();
		}

		@Override
		public String toString() {
			return first + "." + second;
		}

		@Override 
		public int compareTo(Coordinates other){ 
			if (this.first < other.first)
				return -1;
			else if (this.first == other.first) {
				return (this.second < other.second ? -1 : (this.second == other.second ? 0 : 1));
			} else
				return 1;
		}
	}