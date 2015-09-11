package org.apache.hadoop.mapreduce.approx.multistage;

import java.io.IOException;

import java.util.Iterator;
import java.util.Date;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Double;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.commons.math.distribution.TDistribution;
import org.apache.commons.math.distribution.TDistributionImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.util.Progressable;

import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;

import org.apache.hadoop.mapreduce.approx.ApproximateLongWritable;
import org.apache.hadoop.mapreduce.approx.ApproximateIntWritable;

import org.apache.log4j.Logger;

/**
 * Perform multistage sampling.
 * It gets the data from the reducers and performs sampling on the fly.
 * The data needs to come properly sorted.
 * @author Inigo Goiri
 */
public abstract class ApproximateReducer<KEYIN extends Text, VALUEIN, KEYOUT, VALUEOUT extends WritableComparable> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	private static final Logger LOG = Logger.getLogger(ApproximateReducer.class);
	
	public static final String NULLKEY = "NULLKEY";
	public static final char MARK_PARAM = 'w';
	// If we are doing the precise or execution
	protected boolean precise = false;
	
	// Keep track of what was the last key
	protected String prevKey = null;
	
	protected int reducerId = 0;
	protected int n = 0;
	
	// cluster total
	protected ArrayList<Double> ti;
	// cluster weight
	protected ArrayList<Double> wi;
	
	
	// Score based on T-student distribution for estimating the range
	protected double tscore = 1.96; // Default is 95% for high n

	protected double total;
	protected double variance;
	protected double s2;
	
	/**
	 * Initialize all the variables needed for multistage sampling.
	 */
	@Override
	public void setup(Context context) {
		// Check if we are precise or not
		Configuration conf = context.getConfiguration();
		precise = conf.getBoolean("mapred.job.precise", false);
		
		reducerId = context.getTaskAttemptID().getTaskID().getId();
		
		// If we are approximating, we use the rest
		if (!precise) {
			ti = new ArrayList<Double>();
			wi = new ArrayList<Double>();
		}
	}
	
	/**
	 * This is a wrapper for Context that gets values and clusters them if required.
	 */
	public class ClusteringContext extends Context {
		protected Context context;
		
		/**
		 * We need to store the wrapped context to forward everything.
		 */
		public ClusteringContext(Context context) throws IOException, InterruptedException {
			// This is just a wrapper, so we don't create anything
			super(context.getConfiguration(), context.getTaskAttemptID(), null, null, null, null, context.getOutputCommitter(), null, (RawComparator<KEYIN>) context.getSortComparator(), (Class<KEYIN>) context.getMapOutputKeyClass(), (Class<VALUEIN>) context.getMapOutputValueClass());

			// Save the wrapped context
			this.context = context;
		}
		
		/**
		 * Estimate and save the current result.
		 */
		public void saveCurrentResult() throws IOException,InterruptedException {
			double[] result = estimateCurrentResult();
			double tauhat   = result[0];
			double interval = result[1];
			

			context.write((KEYOUT) new Text(prevKey), (VALUEOUT) new Text(String.format("%.2f;error:%.2f;s2:%.2f", tauhat, interval, s2)));
		}
		
		/**
		 * Overwrite of regular write() to capture values and do clustering if needed. If we run precise, pass it to the actual context.
		 */
		@Override
		public void write(KEYOUT key, VALUEOUT value) throws IOException,InterruptedException {
			// For precise, we just forward it to the regular context
			// In this implementation, the marks are always Strings
			if (isPrecise() || !(key instanceof Text)) {
				context.write(key, value);
			// If we don't want precise, we save for multistage sampling
			} else {
				// We have to convert the writtable methods into numbers
				Double res = 0.0;
				if (value instanceof LongWritable) {
					res = new Double(((LongWritable) value).get());
				} else if (value instanceof IntWritable) {
					res = new Double(((IntWritable) value).get());
				} else if (value instanceof DoubleWritable) {
					res = new Double(((DoubleWritable) value).get());
				} else if (value instanceof FloatWritable) {
					res = new Double(((FloatWritable) value).get());
				}
				
				// Extract the actual key
				String keyStr = ((Text) key).toString();
				int lastIndex = keyStr.lastIndexOf('-');
				byte[] aux = keyStr.getBytes();
							
				// if it's weight k,v
				if (keyStr.charAt(lastIndex+1) == MARK_PARAM && keyStr.length() == lastIndex + 2) {
					String origKey = new String(aux, 0, aux.length-6);
					// We changed the key and the previous one wasn't a parameter, write it!
					if (!origKey.equals(prevKey) && prevKey != null) {
						saveCurrentResult();
						
					}
					wi.add(res);
					prevKey = origKey;
					
				} else {
					String origKey = new String(aux, 0, aux.length-4);
					// We changed the key and the previous one wasn't a parameter, write it!
					if (!origKey.equals(prevKey) && prevKey != null) {
						saveCurrentResult();
					}
					ti.add(res);
					prevKey = origKey;
				}
			}
		}

		// We overwrite these method to avoid problems, ideally we would forward everything
		public void getfirst() throws IOException { }
		
		public float getProgress() {
			return context.getProgress();
		}
		
		public void progress() {
			context.progress();
		}
		
		public void setStatus(String status) {
			context.setStatus(status);
		}
		
		public Counter getCounter(Enum<?> counterName) {
			return context.getCounter(counterName);
		}
		
		public Counter getCounter(String groupName, String counterName) {
			return context.getCounter(groupName, counterName);
		}
	}
	
	/**
	 * Reduce runner that uses a thread to check the results.
	 */
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		
		// Precise
		if (isPrecise()) {
			while (context.nextKey()) {
				KEYIN key = context.getCurrentKey();
				reduce(key, context.getValues(), context);
			}
		// Sampling
		} else {
			// We wrap the context to capture the writing of the keys
			ClusteringContext clusteringContext = getClusteringContext(context);
			
			while (context.nextKey()) {
				KEYIN key = context.getCurrentKey();
				// Note that we pass the parameters over the reducer too
				reduce(key, context.getValues(), clusteringContext);
			}
			
			// We need to treat the last key (not parameters)
			if (prevKey != null) {
				clusteringContext.saveCurrentResult();
			}
		}
		
		cleanup(context);
	}
	
	/**
	 * Wrapper to create a new clustering context according to the user specs.
	 * It returns a regular context that outputs the estimated result.
	 */
	protected ClusteringContext getClusteringContext(Context context) throws IOException, InterruptedException {
		return new ClusteringContext(context);
	}
	
	
	
	/**
	 * Check if we run the job precisely.
	 */
	public void setPrecise() {
		this.precise = true;
	}
	
	public boolean isPrecise() {
		return this.precise;
	}
	
	/**
	 * Get the estimated result for the current key.
	 * @return [tauhat, tscore*setauhat]
	 */
	protected double[] estimateCurrentResult() {
		return estimateCurrentResult(true);
	}
	
	protected double[] estimateCurrentResult(boolean reset) {
		// Estimate the result
		double sum = 0.0;
		s2 = 0.0;
		for (int i = 0; i < ti.size(); i++ ) {
			sum += ti.get(i).doubleValue()/wi.get(i).doubleValue();
		}
		total = sum/ti.size();
		sum = 0.0;
		for (int i = 0; i < ti.size(); i++ ) {
			sum += Math.pow(ti.get(i).doubleValue()/wi.get(i).doubleValue() - total, 2);
		}
		s2 = sum/(ti.size()-1);
		variance = Math.sqrt(s2/n);
		wi.clear();
		ti.clear();
		return new double[] {total, tscore*variance};
	}
	
}
