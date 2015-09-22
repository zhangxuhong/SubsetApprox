package org.apache.hadoop.mapreduce.approx;

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
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;

import org.apache.hadoop.mapreduce.approx.ApproximateLongWritable;
import org.apache.hadoop.mapreduce.approx.ApproximateIntWritable;
import org.apache.hadoop.mapreduce.approx.ApproximateDoubleWritable;

import org.apache.log4j.Logger;

/**
 * Perform multistage sampling.
 * It gets the data from the reducers and performs sampling on the fly.
 * The data needs to come properly sorted.
 * @author Inigo Goiri
 */
public abstract class ApproximateReducer<KEYIN extends Text, VALUEIN, KEYOUT, VALUEOUT extends WritableComparable> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	private static final Logger LOG = Logger.getLogger("Subset.Reducer");
	
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
	// size of each cluster
	protected ArrayList<Long> mi;
	// mean of each cluster
	//protected ArrayList<Double> yi_mean;
	// s2 within a cluster
	protected ArrayList<Double> sw;
	
	
	// Score based on T-student distribution for estimating the range
	protected double tscore = 1.96; // Default is 95% for high n
	protected double confidence;
	protected double error;

	protected double total;
	protected double variance;
	protected double s2;
	protected long totalSize;
	protected boolean isWeight;
	
	protected double msb;
	protected double msw;
	protected double sst;
	protected double k;
	protected long weightedSize;
	protected double deff_p;
	protected double deff_c;
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
			mi = new ArrayList<Long>();
			//yi_mean = new ArrayList<Double>();
			sw = new ArrayList<Double>();
			isWeight = false;
			error = Double.parseDouble(conf.get("mapred.job.error", "1.0"));
			confidence = Double.parseDouble(conf.get("mapred.job.confidence", "-1.0"));
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
		public ClusteringContext(Context context, RawKeyValueIterator rIter) throws IOException, InterruptedException {
			// This is just a wrapper, so we don't create anything
			super(context.getConfiguration(), context.getTaskAttemptID(), rIter, null, null, null, context.getOutputCommitter(), null, (RawComparator<KEYIN>) context.getSortComparator(), (Class<KEYIN>) context.getMapOutputKeyClass(), (Class<VALUEIN>) context.getMapOutputValueClass());

			// Save the wrapped context
			this.context = context;
		}
		
		/**
		 * Estimate and save the current result.
		 */


		public void saveCurrentResult() throws IOException,InterruptedException {
			// key need to be consistent with segmap
			if(conf.getBoolean("map.input.sample.pilot", false)){
				context.getCounter("sampleSize", prevKey).setValue(estimateSampleSize());
			}
			double[] result = estimateCurrentResult();
			double tauhat   = result[0];
			double interval = result[1];
			context.write((KEYOUT) new Text(prevKey), (VALUEOUT) new ApproximateDoubleWritable(tauhat, interval));
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
					if(wi.size() == ti.size() +  1){
						ti.add(ti.get(ti.size()-1));
					}
					prevKey = origKey;
					isWeight = true;
					
				} else {
					String origKey = new String(aux, 0, aux.length-4);
					// We changed the key and the previous one wasn't a parameter, write it!
					isWeight = false;
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
		LOG.info("Reduce taskID:" + String.valueOf(context.getTaskAttemptID().getTaskID().getId()));
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
				// handle duplicated segments
				LOG.info(key.toString());
				if(ti.size() == mi.size() + 1){
					sw.add(sw.get(sw.size() -1));
					totalSize += mi.get(mi.size()-1).longValue();
					mi.add(mi.get(mi.size()-1));
				}
				reduce(key, context.getValues(), clusteringContext);
				if(!isWeight){
					calculateClusterMean(context.getValues());
				}
				
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
		RawKeyValueIterator rIter = new RawKeyValueIterator() {
	      public void close() throws IOException {
	        //rawIter.close();
	      }
	      public DataInputBuffer getKey() throws IOException {
	        return null;
	      }
	      public Progress getProgress() {
	        return null;
	      }
	      public DataInputBuffer getValue() throws IOException {
	        return null;
	      }
	      public boolean next() throws IOException {
	        return true;
	      }
	    };
		return new ClusteringContext(context, rIter);
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

	private long estimateSampleSize(){
		double y_mean = 0.0;
		int clusterSize = ti.size();
		deff_p = 0.0;
		double wi_sum = 0.0;
		for(int i = 0; i < clusterSize; i++){
			y_mean += ti.get(i).doubleValue();
			wi_sum += wi.get(i).doubleValue();
			deff_p += Math.pow(wi.get(i).doubleValue(), 2);
		}
		deff_p = deff_p / Math.pow(wi_sum, 2);
		y_mean = y_mean / totalSize;
		msb = 0.0;
		msw = 0.0;
		k = 0.0;
		weightedSize = 0;
		for(int i = 0; i < clusterSize; i++){
			long ni = mi.get(i).longValue();
			msb += Math.pow((ti.get(i).doubleValue()/ni - y_mean), 2) * ni;
			msw += sw.get(i).doubleValue();
			k += Math.pow(ni, 2);
			weightedSize += ni * wi.get(i).doubleValue();
		}
		sst = msb + msw;
		msb = msb / (clusterSize - 1);
		msw = msw / (totalSize - clusterSize);
		k = (1/(clusterSize -1)) * (totalSize - k / totalSize);
		double p_aov = (msb - msw) / (msb + (k-1)*msw);
		deff_c = 1 + (weightedSize -1) * p_aov;

		//calculate size 
		if(confidence > 0){
			tscore = getTScore(totalSize-1, confidence);
		}
		long srs = (long)Math.ceil((sst/(totalSize-1) * Math.pow(tscore, 2)) / Math.pow(error, 2));
		mi.clear();
		sw.clear();
		//yi_mean.clear()
		return (long)Math.ceil((deff_p * deff_c) * srs);
	}

	private void calculateClusterMean(Iterable<VALUEIN> values) {
		double sum = 0.0;
		long numRecords = 0;
		for(VALUEIN value: values){
			double val = 0.0;
			if (value instanceof LongWritable) {
				val = (double)(((LongWritable) value).get());
			} else if (value instanceof IntWritable) {
				val = (double)(((IntWritable) value).get());
			} else if (value instanceof DoubleWritable) {
				val = (double)(((DoubleWritable) value).get());
			} else if (value instanceof FloatWritable) {
				val = (double)(((FloatWritable) value).get());
			}
			numRecords++;
			sum += val;
		}
		double mean = sum / numRecords;
		//yi_mean.add(mean);
		sum = 0.0;
		for(VALUEIN value: values){
			double val = 0.0;
			if (value instanceof LongWritable) {
				val = (double)(((LongWritable) value).get());
			} else if (value instanceof IntWritable) {
				val = (double)(((IntWritable) value).get());
			} else if (value instanceof DoubleWritable) {
				val = (double)(((DoubleWritable) value).get());
			} else if (value instanceof FloatWritable) {
				val = (double)(((FloatWritable) value).get());
			}
			sum += Math.pow((val - mean), 2);
		}
		sw.add(sum);
		totalSize += numRecords;
		mi.add(numRecords);
	}


	private double getTScore(long degrees, double confidence) {
		double tscore = 1.96; // By default we use the normal distribution
		try {
			TDistribution tdist = new TDistributionImpl(degrees);
			//double confidence = 0.95; // 95% confidence => 0.975
			tscore = tdist.inverseCumulativeProbability(1.0-((1.0-confidence)/2.0)); // 95% confidence 1-alpha
		} catch (Exception e) { }
		return tscore;
	}
	
}
