package org.apache.hadoop.mapreduce.approx.app;

import java.lang.Exception;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import org.apache.hadoop.mapreduce.approx.ApproximatePartitioner;
import org.apache.hadoop.mapreduce.approx.ApproximateMapper;
import org.apache.hadoop.mapreduce.approx.ApproximateReducer;
import org.apache.hadoop.mapreduce.approx.lib.input.MySampleTextInputFormat;

import org.apache.log4j.Logger;

import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
import org.json.simple.parser.*;

public class AmazonReview {
	/**
	 * Launch wikipedia page rank.
	 */
	public static class AmazonReviewMapper1 extends ApproximateMapper<LongWritable, Text, Text, DoubleWritable> {
		private static final Logger LOG = Logger.getLogger("Subset.AppMapper");
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				JSONParser parser = new JSONParser();
				JSONObject line = null;
				try{
					line = (JSONObject)parser.parse(value.toString());
				} catch (org.json.simple.parser.ParseException e){
					e.printStackTrace();
				}
				String filter = "Books";
				if(line.containsKey("salesRank")){
					JSONObject salesRank = (JSONObject)line.get("salesRank");
					Set<String> keyset = (Set<String>)salesRank.keySet();
					for(String categ: keyset){
						if(categ.equals(filter)){
							DoubleWritable quantity = new DoubleWritable(0.0);
							if(line.containsKey("reviewText")){
								String review = (String)line.get("reviewText");
								StringTokenizer st = new StringTokenizer(review);
								int size =  st.countTokens();
								quantity.set((double)size);
							}
							context.write(new Text(filter), quantity);
						}
					}
			}
			
		}
	}
	public static class AmazonReviewMapper extends ApproximateMapper<LongWritable, Text, Text, DoubleWritable> {
		private static final Logger LOG = Logger.getLogger("Subset.AppMapper");
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				JSONParser parser = new JSONParser();
				JSONObject line = null;
				try{
					line = (JSONObject)parser.parse(value.toString());
				} catch (org.json.simple.parser.ParseException e){
					e.printStackTrace();
				}
				String[] whereKeys = context.getConfiguration().get("map.input.where.clause", null).split(Pattern.quote(","));
				String filter1 = whereKeys[0].split("=")[1];
				String filter2 = "";
				if(whereKeys.length == 2){
					filter2 = whereKeys[1].split("=")[1];
				}
				if(line.containsKey("salesRank")){
					JSONObject salesRank = (JSONObject)line.get("salesRank");
					Set<String> keyset = (Set<String>)salesRank.keySet();
					for(String categ: keyset){
						if(categ.equals(filter1)){
							if(filter2.equals("")){
								DoubleWritable quantity = new DoubleWritable(0.0);
								if(line.containsKey("reviewText")){
									String review = (String)line.get("reviewText");
									StringTokenizer st = new StringTokenizer(review);
									int size =  st.countTokens();
									quantity.set((double)size);
								}
								context.write(new Text(filter1), quantity);

							}else{
								if(line.containsKey("reviewTime")){
									String time = (String)line.get("reviewTime");
									if(time.contains(filter2)){
										DoubleWritable quantity = new DoubleWritable(0.0);
										if(line.containsKey("reviewText")){
											String review = (String)line.get("reviewText");
											StringTokenizer st = new StringTokenizer(review);
											int size =  st.countTokens();
											quantity.set((double)size);
										}
										context.write(new Text(filter2+ "+*+" + filter1), quantity);

									}
								}
							}
						}
					}
			}
			
		}
	}
	public static class AmazonReviewReducer extends ApproximateReducer<Text, DoubleWritable, Text, DoubleWritable> {
		DoubleWritable result = new DoubleWritable();
		private static final Logger LOG = Logger.getLogger("Subset.AppReducer");
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			long count = 0;
			for(DoubleWritable val : values){
				sum += val.get();
				count++;
			}
			if(context.getConfiguration().get("mapred.sampling.app", "total").equals("total")){
				result.set(sum);
			}else {
				result.set(sum/count);
			}
			LOG.info("precise result:"+result.toString());
			context.write(key, result);
		}
	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Parsing options
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Options options = new Options();
		options.addOption("t", "table", true, "table name");
		options.addOption("w", "where", true, "where clause");
		options.addOption("g", "groupBy", true, "groupBy clause");
		options.addOption("r", "ratio", true, "sampling ratio");
		options.addOption("e", "error", true, "sampling error");
		options.addOption("c", "confidence", true, "confidence level");
		options.addOption("i", "input",    true,  "Input file");
		options.addOption("o", "output",   true,  "Output file");
		options.addOption("f", "filter", true, "filter keyword");
		options.addOption("s", "size", true, "sampling size");
		options.addOption("p", "precise", false, "disable approximation");
		options.addOption("m", "max", true, "max split size");
		options.addOption("b", "block", false, "block unit");
		options.addOption("q", "equal", false, "equal probability");
		options.addOption("x", "equalsize", true, "seg size for equal probability");
		options.addOption("d", "deff",false,"enable deff estimate");
		options.addOption("a", "app", true, "average or sum");

		try {
			CommandLine cmdline = new GnuParser().parse(options, otherArgs);
			String input  = cmdline.getOptionValue("i");
			String output = cmdline.getOptionValue("o");
			int numReducer = 1;
			boolean isError = false;
			boolean isPrecise = false;
			//long maxsize = 67108864;
			if (input == null || output == null) {
				throw new ParseException("No input/output option");
			}
			if(cmdline.hasOption("a")){
				conf.set("mapred.sampling.app", cmdline.getOptionValue("a"));
			}
			if(cmdline.hasOption("d")){
				conf.setBoolean("mapred.sample.deff", true);
			}
			if(cmdline.hasOption("x")){
				conf.setLong("map.input.sampling.equal.size", Long.parseLong(cmdline.getOptionValue("x")));
			}
			if(cmdline.hasOption("b")){
				conf.setBoolean("map.input.block.unit", true);
			}
			if(cmdline.hasOption("q")){
				conf.setBoolean("map.input.sampling.equal", true);
			}
			if(cmdline.hasOption("t")) {
				conf.set("map.input.table.name", cmdline.getOptionValue("t"));
			}
			if(cmdline.hasOption("w")) {
				conf.set("map.input.where.clause", cmdline.getOptionValue("w"));
				conf.set("map.input.filter", cmdline.getOptionValue("w").split("=")[1]);
			}
			if(cmdline.hasOption("g")) {
				conf.set("map.input.groupby.clause", cmdline.getOptionValue("g"));
			}
			if(cmdline.hasOption("r")){
				conf.setBoolean("map.input.sampling.ratio", true);
				conf.setBoolean("map.input.sampling.error", false);
				conf.set("map.input.sampling.ratio.value", cmdline.getOptionValue("r"));
			}
			if(cmdline.hasOption("s")){
				conf.setLong("map.input.sample.size", Long.parseLong(cmdline.getOptionValue("s")));
				conf.setBoolean("map.input.sampling.error", false);
			}
			if(cmdline.hasOption("e")){
				isError = true;
				conf.set("mapred.job.error",cmdline.getOptionValue("e"));
				conf.set("mapred.job.confidence",cmdline.getOptionValue("c"));
				conf.setBoolean("map.input.sampling.error", true);
			}
			if(cmdline.hasOption("p")){
				conf.setBoolean("mapred.job.precise", true);
				isPrecise = true;
			}
			if(cmdline.hasOption("m")){
				conf.setLong("mapreduce.input.fileinputformat.split.maxsize", Long.parseLong(cmdline.getOptionValue("m")));
			}

			if(isPrecise){
				Job job = new Job(conf, "total of AmazonReview");
				job.setJarByClass(AmazonReview.class);
				//job.setNumReduceTasks(numReducer);
				job.setMapperClass(AmazonReviewMapper.class);
				job.setReducerClass(AmazonReviewReducer.class);

				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(DoubleWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);

				job.setPartitionerClass(HashPartitioner.class);

				job.setInputFormatClass(TextInputFormat.class);

				FileInputFormat.setInputPaths(job,   new Path(input));
				FileOutputFormat.setOutputPath(job, new Path(output));
				job.waitForCompletion(true);
				return;
			}

			//cmdline.getOptionValue
			if(isError){
				Configuration pilotConf = new Configuration(conf);
				pilotConf.setLong("map.input.sample.size", 1000);
				pilotConf.setBoolean("map.input.sample.pilot", true);
				Job job = new Job(pilotConf, "pilot");
				job.setJarByClass(AmazonReview.class);
				//job.setNumReduceTasks(numReducer);
				job.setMapperClass(AmazonReviewMapper.class);
				job.setReducerClass(AmazonReviewReducer.class);

				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(LongWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);

				job.setPartitionerClass(ApproximatePartitioner.class);

				job.setInputFormatClass(MySampleTextInputFormat.class);

				FileInputFormat.setInputPaths(job,   new Path(input));
				FileOutputFormat.setOutputPath(job, new Path(output+"/pilot"));
				job.waitForCompletion(true);
				//estimate new size according to pilot and error pilotConfidence
				CounterGroup cg = job.getCounters().getGroup("sampleSize");
				Iterator<Counter> iterator = cg.iterator();
				while(iterator.hasNext()){
					Counter ct = iterator.next();
					pilotConf.setLong("map.input.sample.size." + ct.getName(), ct.getValue());
				}
			}

			Job job = new Job(conf, "total of AmazonReview");
			job.setJarByClass(AmazonReview.class);
			//job.setNumReduceTasks(numReducer);
			job.setMapperClass(AmazonReviewMapper.class);
			job.setReducerClass(AmazonReviewReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			job.setPartitionerClass(ApproximatePartitioner.class);

			job.setInputFormatClass(MySampleTextInputFormat.class);

			FileInputFormat.setInputPaths(job,   new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);


		} catch (ParseException exp){
			System.err.println("Error parsing command line: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(AmazonReview.class.toString(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
	}

}