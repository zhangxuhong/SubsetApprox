package org.apache.hadoop.mapreduce.approx.index;

import java.lang.Exception;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import org.apache.hadoop.mapreduce.approx.index.IndexPartitioner;
import org.apache.hadoop.mapreduce.approx.index.IndexMapper;
import org.apache.hadoop.mapreduce.approx.index.IndexReducer;

import org.apache.log4j.PropertyConfigurator;

public class IndexTpch {
	/**
	 * Launch wikipedia page rank.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//PropertyConfigurator.configure("conf/log4j.properties");
		// Parsing options
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Options options = new Options();
		options.addOption("s", "segment", true, "segment size");
		options.addOption("d", "delimiter", true, "fields delimiter");
		options.addOption("f", "fields", true, "fields to compute histogram, ex. 0-2-4, 0th,2th,4th field");
		options.addOption("i", "input",    true,  "Input file");
		options.addOption("t", "table",   true,  "table name");

		try {
			CommandLine cmdline = new GnuParser().parse(options, otherArgs);
			String input  = cmdline.getOptionValue("i");
			String table = cmdline.getOptionValue("t");
			String output = "/index/table/"+table;
			int numReducer = 1;
			if (input == null || output == null) {
				throw new ParseException("No input/output option");
			}
			if(cmdline.hasOption("s")) {
				long segSize = Long.parseLong(cmdline.getOptionValue("s"));
				conf.setLong("map.input.segment.size", segSize);
			}
			if(cmdline.hasOption("d")) {
				conf.set("map.input.delimiter", cmdline.getOptionValue("d"));
			}
			if(cmdline.hasOption("f")) {
				conf.set("map.input.index.fields", cmdline.getOptionValue("f"));
				numReducer = cmdline.getOptionValue("f").split("-").length;
			}

			//cmdline.getOptionValue

			Job job = new Job(conf, "Build histogram");
			job.setJarByClass(IndexTpch.class);
			job.setNumReduceTasks(numReducer);
			job.setMapperClass(IndexTpchMapper.class);
			job.setReducerClass(IndexReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			job.setPartitionerClass(IndexPartitioner.class);

			job.setInputFormatClass(TextInputFormat.class);

			FileInputFormat.setInputPaths(job,   new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (ParseException exp){
			System.err.println("Error parsing command line: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(IndexTpch.class.toString(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(2);
		}
	}

}