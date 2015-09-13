package org.apache.hadoop.mapreduce.approx;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;

public class SegmentsMap {

  private Configuration conf;
  private Path path;

  public SegmentsMap (Configuration conf, Path path){
    this.conf = conf;
    this.path = path;
  }
  public static class Segment {

    // ***************some static info retrieved from index file.***************************
		private long offset;
		private long length;
    private long rows;
    private long[] frequency;
    private String[] keyword;

    public Segment(long offset, long length, long rows, long[] frequency, String[] keyword){
      this. offset =offset;
      this.length = length;
      this.rows = rows;
      this.frequency = frequency;
      this.keyword = keyword;
    }
    public void setOffset(long offset){
      this. offset =offset;
    }
    public void setLength(long length){
      this.length = length;
    }
    public void setRows(long rows){
      this.rows = rows;
    }
    public void setFrequency(long[] frequency){
      this.frequency = frequency;
    }
    public void setKeyword(String[] keyword){
      this.keyword = keyword;
    }

    public double getProbability(String key){
      return (double)frequency[0]/rows;
    }
    public long getRows(){
      return rows;
    }
    public long[] getFrequency(){
      return frequency;
    }
    public long getOffset(){
      return offset;
    }
    public long getLength(){
      return length;
    }
   
    //*************** info used for sampling******************************************
    private String keys;
    private String weights;

    public void addWeight(double weight){
      weights = weights + "*+*" + String.valueOf(weight);
    }

    public void addKey(String onekey){

      keys = keys + "*+*" + onekey;
    }

    public String getWeights(){
      return weights;
    }

    public String getKeys(){
      return keys;
    }
  		
  }



	public Segment[] getSampleSegmentsList(double ratio){

    List<Segment> sampleSegmentsList = new ArrayList<Segment>();
		//array need to be sorted based offset.
    String[] filterKeys = null;
    Segment[] keysSegments =  this.retrieveKeyHistogram(filterKeys);
    for(String filterKey : filterKeys){
      this.randomProcess(keysSegments, sampleSegmentsList, filterKey, 10);
    } 
		return sampleSegmentsList.toArray(new Segment[sampleSegmentsList.size()]);
	}

	public Segment[] getSampleSegmentsList(double error, double s2, double confidence){

		return null;
	}


  private void randomProcess(Segment[] keysSegments, List<Segment> sampleSegmentsList, String key, long sampleSize){
    Segment candidate = null;
    double weight = -1;
    //add random process here
    this.addToSampleSegmentList(candidate, sampleSegmentsList, key, weight);
  }

  private void addToSampleSegmentList(Segment candidate, List<Segment> sampleSegmentsList, String key, double weight){
    candidate.addKey(key);
    candidate.addWeight(weight);
    if(!sampleSegmentsList.contains(candidate)){
      sampleSegmentsList.add(candidate);
    }
  }

  private Segment[] retrieveKeyHistogram(String[] keys){
    //read a file sequentially
    try {
      FileSystem fs = FileSystem.get(conf);;
      Path newPath = new Path(path.toString()+"-index");
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(newPath)));
      String line = bufferedReader.readLine();
      while (line != null) {
        System.out.println(line);
        line = bufferedReader.readLine();
      }
    } catch (IOException e) {
      //bufferedReader.close();
      e.printStackTrace();
    }
    return null;
  }
}