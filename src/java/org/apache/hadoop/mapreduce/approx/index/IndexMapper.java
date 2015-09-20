package org.apache.hadoop.mapreduce.approx.index;

import java.io.IOException;
import java.lang.Integer;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static final Logger LOG = Logger.getLogger("Subset");

	private long recordCount;
	private long segSize;
	private Configuration conf;
	private String delimiter;
	private String[] indexFields;
	private List<Hashtable<String, Long>> histogram;
	private List<Hashtable<String, Long>> preHistogram;
	private long segPosition;
	private long preSegPosition;

	public void setup(Context context
                       ) throws IOException, InterruptedException {
		recordCount = 0;
		segPosition = 0;
		preSegPosition = 0;
		conf = context.getConfiguration();
		segSize = conf.getLong("map.input.segment.size", 1000);
		delimiter = conf.get("map.input.delimiter", ",");
		indexFields = conf.get("map.input.index.fields", "0").split("-");
		histogram  = new ArrayList<Hashtable<String, Long>>(indexFields.length);
		for(int i = 0; i < indexFields.length; i++){
			histogram.add(new Hashtable<String, Long>());
		}
		preHistogram = null;
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(recordCount == 0){
			segPosition = key.get();
		}
		String[] fields = value.toString().split(delimiter);
		//int[] index = new int[indexFields.length];
		for(int i = 0; i < indexFields.length; i++){
			int index = Integer.parseInt(indexFields[i]);
			String keyword = fields[index];
			Long preValue = histogram.get(i).get(keyword);
			if(preValue != null){
				histogram.get(i).put(keyword, preValue + 1);
			}
			else{
				histogram.get(i).put(keyword, new Long(1));
			}
		}
		recordCount++;
		if(recordCount == segSize){
			if(preHistogram != null){
				//emit histogram for last segment;
				for(int i = 0; i < preHistogram.size(); i++){
					Set<Entry<String, Long>> entries =  preHistogram.get(i).entrySet();
					for(Entry<String, Long> ent : entries){
						context.write(new Text(ent.getKey() + "--" + String.valueOf(i)), 
							new Text(String.format("%d,%d,%d", 
								preSegPosition, segSize, ent.getValue().longValue())));
					}
				}
			}
			preSegPosition = segPosition;
			preHistogram = histogram;
			histogram = new ArrayList<Hashtable<String, Long>>(indexFields.length);
			for(int i = 0; i < indexFields.length; i++){
				histogram.add(new Hashtable<String, Long>());
			}
			recordCount = 0;
		}

	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
		  while (context.nextKeyValue()) {
		    map(context.getCurrentKey(), context.getCurrentValue(), context);
		  }
		  if(recordCount != 0){
		  	for(int i = 0; i < histogram.size(); i++){
			  	Set<Entry<String, Long>> entries =  histogram.get(i).entrySet();
			  	for(Entry<String, Long> ent : entries){
			  		Long value = preHistogram.get(i).get(ent.getKey());
			  		if(value != null){
			  			preHistogram.get(i).put(ent.getKey(), value + ent.getValue());
			  		}
			  		else{
			  			preHistogram.get(i).put(ent.getKey(), value);
			  		}
			  	}
			  	Set<Entry<String, Long>> pEntries =  preHistogram.get(i).entrySet();
				for(Entry<String, Long> ent : pEntries){
					context.write(new Text(ent.getKey() + "--" + String.valueOf(i)), 
						new Text(String.format("%d,%d,%d", 
							preSegPosition, segSize + recordCount, ent.getValue().longValue())));
				}
		  	}
		  }
		  LOG.info("map done");
		} finally {
		  cleanup(context);
		}
	}
}