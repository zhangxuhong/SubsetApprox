package org.apache.hadoop.mapreduce.approx.index;

import java.io.IOException;
import java.lang.Integer;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class IndexTpchMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static final Logger LOG = Logger.getLogger("Subset");

	private long recordCount;
	private long segSize;
	private Configuration conf;
	private String delimiter;
	private String[] indexFields;
	private List<Hashtable<String, Long>> histogram;
	private List<Hashtable<String, Long>> preHistogram;
	private List<Hashtable<String, Double>> stat;
	private List<Hashtable<String, Double>> preStat;
	private long segPosition;
	private long preSegPosition;
	private JSONParser parser;

	public void setup(Context context
                       ) throws IOException, InterruptedException {
		//LOG.setLevel(Level.INFO);
		recordCount = 0;
		segPosition = 0;
		preSegPosition = 0;
		conf = context.getConfiguration();
		segSize = conf.getLong("map.input.segment.size", 1000);
		delimiter = conf.get("map.input.delimiter", ",");
		//parser = new JSONParser();
		//LOG.info("delimiter:"+delimiter);
		indexFields = conf.get("map.input.index.fields", "0").split("-");
		histogram  = new ArrayList<Hashtable<String, Long>>(indexFields.length);
		stat = new ArrayList<Hashtable<String, Double>>(indexFields.length);
		for(int i = 0; i < indexFields.length; i++){
			histogram.add(new Hashtable<String, Long>());
			stat.add(new Hashtable<String, Double>());
		}
		preHistogram = null;
		preStat = null;
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(segSize == 0){
			String keyword = "";
			String[] line = value.toString().split(Pattern.quote(delimiter));
			for(int i = 0; i < indexFields.length; i++){
				int index = Integer.parseInt(indexFields[i]);
				keyword = "";
				keyword = line[index];
					
				//LOG.info("keyword:" + keyword);
				Long preValue = histogram.get(i).get(keyword);
				if(preValue != null){
					histogram.get(i).put(keyword, preValue + 1);
				}
				else{
					histogram.get(i).put(keyword, new Long(1));
				}
			}
			recordCount++;
			return;
		}


		if(recordCount == segSize){
			if(preHistogram != null){
				//emit histogram for last segment;
				long currentPosition = key.get();
				for(int i = 0; i < preHistogram.size(); i++){
					Set<Entry<String, Long>> entries =  preHistogram.get(i).entrySet();
					for(Entry<String, Long> ent : entries){
						context.write(new Text(ent.getKey() + "++" + String.valueOf(preSegPosition) + "--" + String.valueOf(i)), 
							new Text(String.format("%d,%d,%d,%d,%s", 
								preSegPosition, segPosition - preSegPosition, segSize, ent.getValue().longValue(),
								preStat.get(i).get(ent.getKey()).doubleValue())));
						//LOG.info("entry:" + ent.getKey());
					}
				}
			}
			preSegPosition = segPosition;
			preHistogram = histogram;
			preStat = stat;
			histogram = new ArrayList<Hashtable<String, Long>>(indexFields.length);
			stat = new ArrayList<Hashtable<String, Double>>(indexFields.length);
			for(int i = 0; i < indexFields.length; i++){
				histogram.add(new Hashtable<String, Long>());
				stat.add(new Hashtable<String, Double>());
			}
			recordCount = 0;
		}
		if(recordCount == 0){
			segPosition = key.get();
		}
		//String[] fields = (value.toString()).split(Pattern.quote(delimiter));
		//LOG.info("size:"+fields.length);
		//int[] index = new int[indexFields.length];
		
		String keyword = "";
		String[] line = value.toString().split(Pattern.quote(delimiter));
		Double price = Double.parseDouble(line[5]);
			for(int i = 0; i < indexFields.length; i++){
				int index = Integer.parseInt(indexFields[i]);
				keyword = "";
				keyword = line[index];
				//LOG.info("keyword:" + keyword);
				Long preValue = histogram.get(i).get(keyword);
				Double sum = stat.get(i).get(keyword);
				if(preValue != null){
					histogram.get(i).put(keyword, preValue + 1);
					stat.get(i).put(keyword, sum + price);
				}
				else{
					histogram.get(i).put(keyword, new Long(1));
					stat.get(i).put(keyword, price);
				}
		}
		recordCount++;
		

	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
		  while (context.nextKeyValue()) {
		    map(context.getCurrentKey(), context.getCurrentValue(), context);
		  }
		  if(segSize == 0){
		  	FileSplit split = (FileSplit)context.getInputSplit();
		  	for(int i = 0; i < histogram.size(); i++){
		  		Set<Entry<String, Long>> entries =  histogram.get(i).entrySet();
		  		for(Entry<String, Long> ent : entries){
		  			context.write(new Text(ent.getKey()  + "++" + String.valueOf(preSegPosition) + "--" + String.valueOf(i)), 
						new Text(String.format("%d,%d,%d,%d", 
							split.getStart(), split.getLength(), recordCount, ent.getValue().longValue())));
		  		}
		  	}
		  	recordCount = 0;
		  }
		  if(recordCount != 0){
		  	for(int i = 0; i < histogram.size(); i++){
			  	Set<Entry<String, Long>> entries =  histogram.get(i).entrySet();
			  	for(Entry<String, Long> ent : entries){
			  		Long value = preHistogram.get(i).get(ent.getKey());
			  		Double sum = preStat.get(i).get(ent.getKey());
			  		if(value != null){
			  			preHistogram.get(i).put(ent.getKey(), value + ent.getValue());
			  			preStat.get(i).put(ent.getKey(), sum + stat.get(i).get(ent.getKey()));
			  		}
			  		else{
			  			preHistogram.get(i).put(ent.getKey(), ent.getValue());
			  			preStat.get(i).put(ent.getKey(), stat.get(i).get(ent.getKey()));
			  		}
			  	}
			  	Set<Entry<String, Long>> pEntries =  preHistogram.get(i).entrySet();
			  	FileSplit split = (FileSplit)context.getInputSplit();
			  	long currentPosition = split.getStart() + split.getLength();
				for(Entry<String, Long> ent : pEntries){
					context.write(new Text(ent.getKey()  + "++" + String.valueOf(preSegPosition) + "--" + String.valueOf(i)), 
						new Text(String.format("%d,%d,%d,%d,%s", 
							preSegPosition, currentPosition - preSegPosition, segSize + recordCount, ent.getValue().longValue(), 
							preStat.get(i).get(ent.getKey()).doubleValue())));
				}
		  	}
		  }
		  //LOG.info("map done");
		} finally {
		  cleanup(context);
		}
	}
}