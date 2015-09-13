import java.io.IOException;
import java.lang.Integer;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IndexMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String delimiter = conf.get("map.input.delimiter", ",");
		String[] fields = value.toString().split(delimiter);
		String[] indexFields = conf.get("map.input.index.fields", "0").split("-");
		//int[] index = new int[indexFields.length];
		for(String indexField : indexFields){
			int index = Integer.parseInt(indexField);
			String key = fields[index];

		}

	}
}