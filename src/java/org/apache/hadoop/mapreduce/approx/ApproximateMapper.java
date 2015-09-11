package org.apache.hadoop.mapreduce.approx;

import java.io.IOException;
import java.lang.Double;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

import org.apache.hadoop.mapred.JobClient;

import org.apache.hadoop.mapreduce.approx.lib.input.SampleFileSplit;
import org.apache.hadoop.mapreduce.approx.lib.input.SampleLineRecordReader;
import org.apache.hadoop.mapreduce.approx.lib.input.SampleRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.log4j.Logger;

/**
 * 
 */
public abstract class ApproximateMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT extends WritableComparable> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	private static final Logger LOG = Logger.getLogger(ApproximateMapper.class);
	
	/**
	 * This is a wrapper for Context that gets keys and adds an ID at the end to identify the cluster the data comes from.
	 * Incremental doesn't require this only the default reducer.
	 */
	public class ApproxContext extends Context {
		Context context;
		int sendTaskId = 0;
		boolean precise = false;
		
		public ApproxContext(Context context) throws IOException, InterruptedException {
			// This is just a wrapper, so we don't create anything
			super(context.getConfiguration(), context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, null);

			// Save the context
			this.context = context;
			this.sendTaskId = context.getTaskAttemptID().getTaskID().getId();
			this.precise = context.getConfiguration().getBoolean("mapred.job.precise", false);
		}
		
		/**
		 * Overwrite of regular write() to capture values and do clustering if needed. If we run precise, pass it to the actual context.
		 */
		@Override
		public void write(KEYOUT key, VALUEOUT value) throws IOException,InterruptedException {
			if (!this.precise && key instanceof Text) {
				// Sort method with just one more character at the end
				int clusterID = getCurrentClusterID();
				byte[] byteId = new byte[] {(byte) (sendTaskId/128), (byte) (sendTaskId%128), (byte) (clusterID/128), (byte) (clusterID%128)};
				context.write((KEYOUT) new Text(key.toString()+new String(byteId)), value);
				// Long method that is human readable
				//context.write((KEYOUT) new Text(key.toString()+String.format("-%05d", sendTaskId)), value);
			} else {
				context.write(key, value);
			}
		}

		// We overwrite the following methods to avoid problems, ideally we would forward everything
		@Override
		public float getProgress() {
			return context.getProgress();
		}
		
		private int getCurrentClusterID(){
			//RecordReader<KEYIN,VALUEIN> reader = context.getRecordReader();
			int clusterID = -1;
			//if (reader instanceof SampleRecordReader) {
			//	clusterID = ((SampleRecordReader)reader).getCurrentClusterID();
			//}
			return clusterID;
		}

		@Override
		public void progress() {
			context.progress();
		}
		
		@Override
		public void setStatus(String status) {
			context.setStatus(status);
		}
		
		@Override
		public Counter getCounter(Enum<?> counterName) {
			return context.getCounter(counterName);
		}
		
		@Override
		public Counter getCounter(String groupName, String counterName) {
			return context.getCounter(groupName, counterName);
		}
	}
	
	/**
	 * We use this to keep track of the fields and send it to the reducers, the user can decide to use something else or add others.
	 */
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		
		//long t0 = System.currentTimeMillis();
		
		// Create the context that adds an id for clustering (just if requried)
		Context newcontext = context;
		// If we don't do incremental, we have to IDs to the keys
		Configuration conf = context.getConfiguration();
		
		if (!conf.getBoolean("mapred.job.precise", false)) {
			newcontext = new ApproxContext(context);
		}
		
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), newcontext);
		}
		
		cleanup(context);
	}
	
	/**
	 * Cleanup function that reports how many fields have been processed.
	 * This is the default case where each process item is an element m
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		// We send the statistically relevant information to everybody if we are sampling
		Configuration conf = context.getConfiguration();
		if (!conf.getBoolean("mapred.job.precise", false)) {
			// Integer format
			sendWeights(context);
		}
	}

	protected void sendWeights(Context context) throws IOException, InterruptedException {
		SampleFileSplit split = (SampleFileSplit)context.getInputSplit();
		String[] keys = split.getKeys();
		String[] weights = split.getWeights();
		int taskID = context.getTaskAttemptID().getTaskID().getId();
		byte[] byteId1 = new byte[] {(byte) (taskID/128), (byte) (taskID%128)};
		for(int i = 0; i < keys.length; i++){
			byte[] byteId2 = new byte[] {(byte) (i/128), (byte) (i%128)};
			String[] segKeys = keys[i].split("*+*");
			String[] segWeights = weights[i].split("*+*");
			for(int j = 0; j < segKeys.length; j++){
				//may use string builder
				context.write((KEYOUT) new Text(segKeys[j] + new String(byteId1) + new String(byteId2) + "-w"), (VALUEOUT) new DoubleWritable(Double.parseDouble(segWeights[j])));
			}
		}
	}


}
