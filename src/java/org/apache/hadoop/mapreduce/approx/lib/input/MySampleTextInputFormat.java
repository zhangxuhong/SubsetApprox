
package org.apache.hadoop.mapreduce.approx.lib.input;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.approx.lib.input.SampleTextInputFormat;
import org.apache.hadoop.mapreduce.approx.lib.input.SampleRecordReader;
import org.apache.hadoop.mapreduce.approx.lib.input.SampleFileSplit;
import org.apache.hadoop.mapreduce.approx.lib.input.SampleLineRecordReader;

public class MySampleTextInputFormat extends SampleTextInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {

		SampleFileSplit sampleFileSplit = (SampleFileSplit) split;
		SampleRecordReader<LongWritable, Text> recordReader = new SampleRecordReader<LongWritable, Text>(sampleFileSplit, context, SampleLineRecordReader.class);
		try {
			recordReader.initialize(sampleFileSplit, context);
		} catch (InterruptedException e) {
			new RuntimeException("Error to initialize MySampleTextInputFormat.");
		}
		return recordReader;
	}

}