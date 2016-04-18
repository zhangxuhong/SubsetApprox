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
	protected boolean flag = false;
	protected Configuration approxConf;
	// Keep track of what was the last key
	protected String prevKey = null;
	protected String prevSeg = null;
	
	protected int reducerId = 0;
	//protected int n = 0;
	
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
	protected double avg;
	protected double variance;
	//protected double s2;
	protected long totalSize = 0;
	protected boolean isWeight;
	
	protected double msb;
	protected double msw;
	protected double sst;
	protected double k;
	protected double weightedSize;
	protected double deff_p;
	protected double deff_c;
	protected double srsvaricne;
	/**
	 * Initialize all the variables needed for multistage sampling.
	 */
	@Override
	public void setup(Context context) {
		// Check if we are precise or not
		approxConf = context.getConfiguration();
		precise = approxConf.getBoolean("mapred.job.precise", false);
		
		reducerId = context.getTaskAttemptID().getTaskID().getId();
		
		// If we are approximating, we use the rest
		if (!precise) {
			ti = new ArrayList<Double>();
			wi = new ArrayList<Double>();
			mi = new ArrayList<Long>();
			//yi_mean = new ArrayList<Double>();
			sw = new ArrayList<Double>();
			isWeight = false;
			error = Double.parseDouble(approxConf.get("mapred.job.error", "0.01"));
			confidence = Double.parseDouble(approxConf.get("mapred.job.confidence", "-1.0"));
			if(confidence > 0){
				tscore = getTScore(totalSize-1, confidence);
			}
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
			super(approxConf, context.getTaskAttemptID(), rIter, null, null, null, context.getOutputCommitter(), null, (RawComparator<KEYIN>) context.getSortComparator(), (Class<KEYIN>) context.getMapOutputKeyClass(), (Class<VALUEIN>) context.getMapOutputValueClass());

			// Save the wrapped context
			this.context = context;
		}
		
		/**
		 * Estimate and save the current result.
		 */


		public void saveCurrentResult() throws IOException,InterruptedException {
			// key need to be consistent with segmap
			double[] result = estimateCurrentResult();
			if(approxConf.getBoolean("mapred.sample.deff", false)){
				estimateSampleSize();
			}
			if(approxConf.getBoolean("map.input.sample.pilot", false)){
				
				context.getCounter("sampleSize", prevKey).setValue(estimateSampleSize());
			}
			//else{
				
				double tauhat   = result[0];
				double interval = result[1];
				context.write((KEYOUT) new Text(prevKey), (VALUEOUT) new ApproximateDoubleWritable(tauhat, interval));
			wi.clear();
			ti.clear();
			mi.clear();
			sw.clear();
			totalSize = 0;
			//}
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
				if (keyStr.length() == lastIndex + 2 && keyStr.charAt(lastIndex+1) == MARK_PARAM && keyStr.length() == lastIndex + 2) {
					String origKey = new String(aux, 0, aux.length-8);
					String origSeg = new String(aux,0,aux.length-4);
					// We changed the key and the previous one wasn't a parameter, write it!
					if (!origSeg.equals(prevSeg) && prevSeg != null) {
						//saveCurrentResult();
						ti.add(0.0);
						mi.add(new Long(0));
						sw.add(0.0);
						//return;
						
					}
					if(prevSeg == null){
						//prevSeg = origSeg;
						ti.add(0.0);
						mi.add(new Long(0));
						sw.add(0.0);
						//return;
					}
					wi.add(res);
					if(wi.size() == ti.size() +  1){
						ti.add(ti.get(ti.size()-1));
					}
					prevKey = origKey;
					prevSeg = origSeg;
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
					prevSeg = keyStr;
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
			if(approxConf.getBoolean("map.input.sample.pilot", false) || approxConf.getBoolean("mapred.sample.deff", false)){
				while (context.nextKey()) {
					KEYIN key = context.getCurrentKey();
					// handle duplicated segments
					//LOG.info(key.toString());
					
					pliotReduce(key, context.getValues(), clusteringContext);
					
				}
			}
			else{
				while (context.nextKey()) {
					KEYIN key = context.getCurrentKey();
					// handle duplicated segments
					//LOG.info(key.toString());
					 defaultReduce(key, context.getValues(), clusteringContext);	
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
		String app = approxConf.get("mapred.sampling.app", "total");
		double s2 = 0.0;
		variance = 0.0;
		LOG.info("segments:" + String.valueOf(ti.size()));
		LOG.info("totalSize:" + String.valueOf(totalSize));
		if(app.equals("total")){
			//double total;
			double sum = 0.0;
			for (int i = 0; i < ti.size(); i++ ) {
				double weighted = ti.get(i).doubleValue()/wi.get(i).doubleValue();
				sum += weighted;
				//LOG.info("ti/wi:" + String.valueOf(ti.get(i).doubleValue()) + "/" + String.valueOf(wi.get(i).doubleValue()));
				//LOG.info("weighted total:" + String.valueOf(weighted));
			}
			total = sum/ti.size();
			LOG.info("total:" + String.valueOf(total));
			sum = 0.0;
			for (int i = 0; i < ti.size(); i++ ) {
				sum += Math.pow(ti.get(i).doubleValue()/wi.get(i).doubleValue() - total, 2);
			}
			s2 = sum/(ti.size()-1);
			variance = s2/ti.size();
			double std = Math.sqrt(variance);
			LOG.info("var:" + String.valueOf(variance));
			LOG.info("error:" + String.valueOf(tscore*std));
			return new double[] {total, tscore*std};
		}else{
			double sum = 0.0;
			for (int i = 0; i < ti.size(); i++ ) {
				double weighted = ti.get(i).doubleValue()/wi.get(i).doubleValue();
				sum += weighted;
				//LOG.info("ti/wi:" + String.valueOf(ti.get(i).doubleValue()) + "/" + String.valueOf(wi.get(i).doubleValue()));
				//LOG.info("weighted total:" + String.valueOf(weighted));
			}
			sum = sum/ti.size();
			double population = 0.0;
			String real = "";
			String weights = "";
			for(int i = 0; i < mi.size(); i++){
				population += mi.get(i).longValue()/wi.get(i).doubleValue();
				real=real+mi.get(i).toString()+",";
				weights = weights + wi.get(i).toString()+",";

			}
			LOG.info("real:" + real);
			LOG.info("weights:" + weights);
			population = population/mi.size();
			avg = sum/population;
			LOG.info("avg:" + String.valueOf(avg));
			sum = 0.0;
			for (int i = 0; i < ti.size(); i++ ) {
				sum += Math.pow((ti.get(i).doubleValue() - avg * mi.get(i).longValue())/wi.get(i).doubleValue(), 2);
			}
			s2 = sum/Math.pow(population,2);
			s2 = s2 /(ti.size() -1);
			variance = s2/ti.size();
			double std = Math.sqrt(variance);
			LOG.info("var:" + String.valueOf(variance));
			
			LOG.info("error:" + String.valueOf(tscore*std));
			return new double[] {avg, tscore*std};
		}
	}

	private long estimateSampleSize(){
		double y_mean = 0.0;
		int clusterSize = ti.size();
		deff_p = 0.0;
		double wi_sum = 0.0;
		for(int i = 0; i < clusterSize; i++){
			y_mean += ti.get(i).doubleValue();
			wi_sum += (1/wi.get(i).doubleValue())*mi.get(i).longValue();
			deff_p += Math.pow(1/wi.get(i).doubleValue(), 2)*mi.get(i).longValue();
		}
		deff_p = (totalSize * deff_p) / Math.pow(wi_sum, 2);
		LOG.info("deff_p:" + String.valueOf(deff_p));
		//LOG.info("totalSize:" + String.valueOf(totalSize));
		y_mean = y_mean / totalSize;
		//LOG.info("y_mean:" + String.valueOf(y_mean));
		msb = 0.0;
		msw = 0.0;
		k = 0.0;
		weightedSize = 0.0;
		for(int i = 0; i < clusterSize; i++){
			long ni = mi.get(i).longValue();
			if(ni == 0){
				continue;
			}
			msb += Math.pow((ti.get(i).doubleValue()/ni - y_mean), 2) * ni;
			msw += sw.get(i).doubleValue();
			k += Math.pow(ni, 2);
			weightedSize += ni * ((double)ni / totalSize);
		}
		LOG.info("weightedSize:" + String.valueOf(weightedSize));
		sst = msb + msw;
		LOG.info("sst:" + String.valueOf(sst));
		msb = msb / (clusterSize - 1);
		LOG.info("msb:" + String.valueOf(msb));
		msw = msw / (totalSize - clusterSize);
		LOG.info("msw:" + String.valueOf(msw));
		k = (1.0/(clusterSize -1)) * (totalSize - k / (double)totalSize);
		LOG.info("k:" + String.valueOf(k));
		double p_aov = (msb - msw) / (msb + (k-1)*msw);
		LOG.info("p_aov:" + String.valueOf(p_aov));
		deff_c = 1 + (weightedSize -1) * p_aov;
		LOG.info("deff_c:" + String.valueOf(deff_c));

		//calculate size 

		long srs = estimateSRS();
		LOG.info("srs:" + String.valueOf(srs));
		
		//yi_mean.clear()
		double deff = deff_p * deff_c;
		LOG.info("deff:" + String.valueOf(deff));
		long desiredSize = (long)Math.ceil(deff * srs);
		LOG.info("size:" + String.valueOf(desiredSize));
		long desiredSize2 = (long)Math.ceil((variance/srsvaricne) * srs);
		LOG.info("size2:" + String.valueOf(desiredSize2));
		if(desiredSize < desiredSize2){
			return desiredSize;
		}else{
			return desiredSize2;
		}
		
	}

	private long estimateSRS(){
		String app = approxConf.get("mapred.sampling.app", "total");
		long srs = 0;
		srsvaricne = 0.0;
		if(app.equals("total")){
			double population = 0.0;
			for(int i = 0; i < mi.size(); i++){
				population += mi.get(i).longValue()/wi.get(i).doubleValue();
			}
			population = population/mi.size();
			double srsTotalvar = (sst/(totalSize-1))*Math.pow(population,2);
			srsvaricne = srsTotalvar/totalSize;
			LOG.info("srs variance:" + String.valueOf(srsTotalvar/totalSize));
			srs = (long)Math.ceil((srsTotalvar * Math.pow(tscore, 2)) / Math.pow(error*total, 2));
		}else{
			double srsTotalvar = sst/(totalSize-1);
			srs = (long)Math.ceil((srsTotalvar * Math.pow(tscore, 2)) / Math.pow(error*avg, 2));
			srsvaricne = srsTotalvar/totalSize;
			LOG.info("srs variance:" + String.valueOf(srsTotalvar/totalSize));
		}
		return srs;
	}
	private void calculateClusterSw(double mean, ArrayList<VALUEIN> values) {
		//yi_mean.add(mean);
		double sum = 0.0;
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

	protected void pliotReduce(KEYIN key, Iterable<VALUEIN> values, Context context
                        ) throws IOException, InterruptedException {
	    
		double sum = 0.0;
		long numRecords = 0;
		ArrayList<VALUEIN> cache = new ArrayList<VALUEIN>();
		for(VALUEIN value: values){
			cache.add(value);
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
		context.write((KEYOUT) key, (VALUEOUT) new DoubleWritable(sum));

		if(!isWeight){
			totalSize += numRecords;
			//LOG.info("totalSize+:" + String.valueOf(totalSize));
			mi.add(numRecords);  
		    double mean = sum / numRecords;
		    calculateClusterSw(mean, cache);
		}
		if(ti.size() == mi.size() + 1){
			sw.add(sw.get(sw.size() -1));
			totalSize += mi.get(mi.size()-1).longValue();
			//LOG.info("totalSize-:" + String.valueOf(totalSize));
			mi.add(mi.get(mi.size()-1));
		}
  	}

  	protected void defaultReduce(KEYIN key, Iterable<VALUEIN> values, Context context
                        ) throws IOException, InterruptedException {
  		String app = approxConf.get("mapred.sampling.app", "total");
  		double sum = 0.0;
  		long numRecords = 0;
  		if(app.equals("total")){
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
			context.write((KEYOUT) key, (VALUEOUT) new DoubleWritable(sum));
  		}else {
  			
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
			context.write((KEYOUT) key, (VALUEOUT) new DoubleWritable(sum));
		}
		if(!isWeight){
			mi.add(numRecords); 
			totalSize += numRecords; 
		}
		if(ti.size() == mi.size() + 1){
			totalSize += mi.get(mi.size()-1).longValue();
			mi.add(mi.get(mi.size()-1));
		}
  		
  	}
	
}
