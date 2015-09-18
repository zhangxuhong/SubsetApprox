package org.apache.hadoop.mapreduce.approx;

import org.apache.hadoop.io.DoubleWritable;

/**
 * This is just for outputting. The ideal would be to have value as protected in IntWritable but...
 */
public class ApproximateDoubleWritable extends DoubleWritable {
	private double value;
	private double range;

	public ApproximateDoubleWritable(double value, double range) {
		this.value = value;
		this.range = (double) Math.ceil(range);
	}
	
	public String toString() {
		//String.format("%.2f;error:%.2f;s2:%.2f", tauhat, interval, s2))
		return String.valueOf(value)+"+/-"+ String.valueOf(range);
	}

	private double getValue() {
		return this.value;
	}
	
	private double getRange() {
		return this.range;
	}
	
	/**
	 * Normalized error.
	 */
	public double getError() {
		if (this.value == 0) {
			return 0.0;
		}
		return this.range/this.value;
	}
}