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

public class IndexAmazonMapper extends Mapper<LongWritable, Text, Text, Text>{
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
		//delimiter = conf.get("map.input.delimiter", ",");
		parser = new JSONParser();
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
			try{
				JSONObject line = (JSONObject)parser.parse(value.toString());
				for(int i = 0; i < indexFields.length; i++){
					int index = Integer.parseInt(indexFields[i]);
					keyword = "";
					if(index == 0)//date
					{	if(line.containsKey("reviewTime")){
							keyword = (String)line.get("reviewTime");
							keyword = keyword.substring(keyword.length()-4, keyword.length());
						}else{
							continue;
						}
						
					}else if (index == 1) {
						if(line.containsKey("brand")){
							keyword = (String)line.get("brand");
						}
						else{
							continue;
						}
						
					}else if (index == 2) {
						if(line.containsKey("overall")){
							keyword = (String)line.get("overall");
						}
						else{
							continue;
						}
						
					}else if (index == 3) {
						if(line.containsKey("helpful")){
							JSONArray helpful = (JSONArray)line.get("helpful");
							keyword = (String)helpful.get(0);
						}
						else{
							continue;
						}
						
					}else if (index == 4){
						if(line.containsKey("salesRank")){
							JSONObject salesRank = (JSONObject)line.get("salesRank");
							Set<String> keyset = (Set<String>)salesRank.keySet();
							for(String categ: keyset){
								Long preValue = histogram.get(i).get(categ);
								if(preValue != null){
									histogram.get(i).put(categ, preValue + 1);
								}
								else{
									histogram.get(i).put(categ, new Long(1));
								}
							}
						}
						continue;

					}else {
						//String type = "";
						if(line.containsKey("categories")){
							JSONArray type = (JSONArray)line.get("categories");
							keyword = String.valueOf(type.size());
							/*for(int k = 0; k < type.size(); k++){
								JSONArray categories = (JSONArray)type.get(k);
								for(int j = 0; j < categories.size(); j++){
									String categ = (String)categories.get(j);
									Long preValue = histogram.get(i).get(categ);
									if(preValue != null){
										histogram.get(i).put(categ, preValue + 1);
									}
									else{
										histogram.get(i).put(categ, new Long(1));
									}
								}
							}
							continue;*/
						}else{
							continue;
						}
					}
					//LOG.info("keyword:" + keyword);
					Long preValue = histogram.get(i).get(keyword);
					if(preValue != null){
						histogram.get(i).put(keyword, preValue + 1);
					}
					else{
						histogram.get(i).put(keyword, new Long(1));
					}
				}
			} catch (ParseException e){
				e.printStackTrace();
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
		try{
			JSONObject line = (JSONObject)parser.parse(value.toString());
			Double reviewLen = new Double(((String)line.get("reviewText")).length());
				for(int i = 0; i < indexFields.length; i++){
					int index = Integer.parseInt(indexFields[i]);
					keyword = "";
					if(index == 0)//date
					{	if(line.containsKey("reviewTime")){
							keyword = (String)line.get("reviewTime");
							keyword = keyword.substring(keyword.length()-4, keyword.length());
						}else{
							continue;
						}
						
					}else if (index == 1) {
						if(line.containsKey("brand")){
							keyword = (String)line.get("brand");
						}
						else{
							continue;
						}
						
					}else if (index == 2) {
						if(line.containsKey("overall")){
							keyword = line.get("overall").toString();
						}
						else{
							continue;
						}
						
					}else if (index == 3) {
						if(line.containsKey("helpful")){
							JSONArray helpful = (JSONArray)line.get("helpful");
							keyword = helpful.get(0).toString();
						}
						else{
							continue;
						}
						
					}else if (index == 4){
						if(line.containsKey("salesRank")){
							JSONObject salesRank = (JSONObject)line.get("salesRank");
							Set<String> keyset = (Set<String>)salesRank.keySet();
							for(String categ: keyset){
								Long preValue = histogram.get(i).get(categ);
								if(preValue != null){
									histogram.get(i).put(categ, preValue + 1);
								}
								else{
									histogram.get(i).put(categ, new Long(1));
								}
							}
						}
						continue;

					}else {
						//String type = "";
						if(line.containsKey("categories")){
							JSONArray type = (JSONArray)line.get("categories");
							keyword = String.valueOf(type.size());
							/*for(int k = 0; k < type.size(); k++){
								JSONArray categories = (JSONArray)type.get(k);
								for(int j = 0; j < categories.size(); j++){
									String categ = (String)categories.get(j);
									Long preValue = histogram.get(i).get(categ);
									if(preValue != null){
										histogram.get(i).put(categ, preValue + 1);
									}
									else{
										histogram.get(i).put(categ, new Long(1));
									}
								}
							}
							continue;*/
						}else{
							continue;
						}
					}
					//LOG.info("keyword:" + keyword);
					Long preValue = histogram.get(i).get(keyword);
					Double sum = stat.get(i).get(keyword);
					if(preValue != null){
						histogram.get(i).put(keyword, preValue + 1);
						stat.get(i).put(keyword, sum + reviewLen);
					}
					else{
						histogram.get(i).put(keyword, new Long(1));
						stat.get(i).put(keyword, reviewLen);
					}
			}
		} catch (ParseException e){
			e.printStackTrace();
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