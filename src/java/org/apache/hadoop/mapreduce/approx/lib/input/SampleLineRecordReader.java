package org.apache.hadoop.mapreduce.approx.lib.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SampleFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SampleLineRecordReader extends RecordReader<LongWritable, Text> {

	private SampleFileSplit sampleFileSplit;
	private LineRecordReader lineRecordReader = new LineRecordReader();
	//private Path[] paths;
	private int totalLength;
	private int currentIndex;
	private float currentProgress = 0;
	private LongWritable currentKey;
	private Text currentValue = null;

	public SampleLineRecordReader(SampleFileSplit sampleFileSplit, TaskAttemptContext context, Integer index) throws IOException {
		super();
		this.sampleFileSplit = sampleFileSplit;
		this.currentIndex = index; // 当前要处理的小文件Block在SampleFileSplit中的索引
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.sampleFileSplit = (SampleFileSplit) split;
		// 处理SampleFileSplit中的一个小文件Block，因为使用LineRecordReader，需要构造一个FileSplit对象，然后才能够读取数据
		FileSplit fileSplit = new FileSplit(sampleFileSplit.getPath(), sampleFileSplit.getOffset(currentIndex), sampleFileSplit.getLength(currentIndex)-1, sampleFileSplit.getLocations());
		lineRecordReader.initialize(fileSplit, context);

		//this.paths = sampleFileSplit.getPaths();
		totalLength = sampleFileSplit.getNumSegments();
		context.getConfiguration().set("map.input.file.name", sampleFileSplit.getPath().getName());
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		currentKey = lineRecordReader.getCurrentKey();
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		currentValue= lineRecordReader.getCurrentValue();
		return currentValue;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (currentIndex >= 0 && currentIndex < totalLength) {
			return lineRecordReader.nextKeyValue();
		} else {
			return false;
		}
	}

	@Override
	public float getProgress() throws IOException {
		if (currentIndex >= 0 && currentIndex < totalLength) {
			currentProgress = (float) currentIndex / totalLength;
			return currentProgress;
		}
		return currentProgress;
	}

	@Override
	public void close() throws IOException {
		lineRecordReader.close();
	}
}