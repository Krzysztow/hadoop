package org.krzysztow.mapreduce;

import org.krzysztow.mapreduce.Coordinates;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class CoordinatesComparator extends WritableComparator {
	private static final IntWritable.Comparator COORDINATES_COMPARATOR = new IntWritable.Comparator();

	public CoordinatesComparator() {
		super(Coordinates.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1,
			byte[] b2, int s2, int l2) {
		int cmp = COORDINATES_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
		if (0 == cmp)
			return COORDINATES_COMPARATOR.compare(b1, s1 + Integer.SIZE, Integer.SIZE,
					b2, s2 + Integer.SIZE, Integer.SIZE);
		else 
			return cmp;
	}
}