package org.krzysztow.mapreduce;

import org.krzysztow.mapreduce.Coordinates;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class CoordinatesGrouper extends WritableComparator {
	static {
		WritableComparator.define(Coordinates.class, new CoordinatesComparator());
	}
	
	protected CoordinatesGrouper() {
		super(Coordinates.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		int v1 = ((Coordinates)w1).getFirst();
		int v2 = ((Coordinates)w2).getFirst();

		return ((v1 < v2) ? -1 : ((v1 == v2) ? 0 : +1));  
	}

}