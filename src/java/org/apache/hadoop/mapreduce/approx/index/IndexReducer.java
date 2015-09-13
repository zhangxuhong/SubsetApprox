package org.apache.hadoop.mapreduce.approx.index;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class IndexReducer extends Reducer<Text, Text, NullWritable, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text val : values){
			context.write(NullWritable.get(), new Text(key.toString() + "," + val.toString()));
		}
	}
}