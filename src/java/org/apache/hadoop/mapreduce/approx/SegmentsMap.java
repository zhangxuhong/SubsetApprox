package org.apache.hadoop.mapreduce.approx;

import org.apache.hadoop.mapreduce.approx.WeightedRandomSelector;
import org.apache.hadoop.mapreduce.approx.WeightedRandomSelector.WeightedItem;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Hashtable;
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
  private static String FILE_PARENT="/index/table"; // e.g. "hdfs://brick0:54310/metis";
  private static String FILE_PREFIX="part-0000";

  public SegmentsMap (Configuration conf, Path path){
    this.conf = conf;
    this.path = path;
  }
  public static class Segment {

    // ***************some static info retrieved from index file.***************************
		private long offset;
		private long length;
    private long rows;
    private ArrayList<Long> frequency;
    private ArrayList<String> keyword;

    public Segment(long offset, long length, long rows){
      this. offset =offset;
      this.length = length;
      this.rows = rows;
      this.frequency = new ArrayList<Long>();
      this.keyword = new ArrayList<String>();
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
    public void addFrequency(long frequency){
      this.frequency.add(frequency);
    }
    public void addKeyword(String keyword){
      this.keyword.add(keyword);
    }

    public int getKeyWeight(String key){
      //return (double)frequency[0]/rows;
      return 0;
    }
    public long getRows(){
      return rows;
    }
    public long getFrequency(int i){
      return frequency.get(i);
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
    String[] whereKeys = conf.get("map.input.where.clause", null).split(",");
    String groupBy = conf.get("map.input.groupby.clause", null);
    ArrayList<String> filterKeys = new ArrayList<String>();
    Segment[] keysSegments =  this.retrieveKeyHistogram(whereKeys, groupBy, filterKeys);
    List<WeightedItem<Segment>> weightedSegs = new ArrayList<WeightedItem<Segment>>(keysSegments.length);
    for(Segment seg : keysSegments){
      weightedSegs.add(new WeightedItem<Segment>(1, seg));
    }
    for(String filterKey : filterKeys){
      for(WeightedItem<Segment> seg : weightedSegs){
        seg.setWeight(seg.getItem().getKeyWeight(filterKey));
      }
      this.randomProcess(weightedSegs, sampleSegmentsList, filterKey, 10);
    } 
		return sampleSegmentsList.toArray(new Segment[sampleSegmentsList.size()]);
	}

	public Segment[] getSampleSegmentsList(double error, double s2, double confidence){

		return null;
	}


  private void randomProcess(List<WeightedItem<Segment>> weightedSegs, 
                              List<Segment> sampleSegmentsList, String key, long sampleSize) {
    WeightedRandomSelector selector = new WeightedRandomSelector(weightedSegs);
    for(int i = 0; i < sampleSize; i++){
      WeightedItem<Segment> candidate = selector.select();
      double weight = (double)(candidate.getWeight()) / selector.getRangeSize();
      this.addToSampleSegmentList(candidate.getItem(), sampleSegmentsList, key, weight);
    }
    
  }

  private void addToSampleSegmentList(Segment candidate, List<Segment> sampleSegmentsList, String key, double weight){
    candidate.addKey(key);
    candidate.addWeight(weight);
    if(!sampleSegmentsList.contains(candidate)){
      sampleSegmentsList.add(candidate);
    }
  }

  private Segment[] retrieveKeyHistogram(String[] wherekeys, String groupBy, List filterKeys){
    //read a file sequentially
    // format: key, offset, length, segsize, frequency
    //here need to form all the filter keys (cominations of fields)
    // index=key, index
    try {
      Hashtable<String, Segment> segTable = new Hashtable<String, Segment>();
      String tableName = conf.get("map.input.table.name", "");
      String indexfile = this.FILE_PREFIX + "/" + tableName + "/" + this.FILE_PREFIX;
      String filterKey = "";
      for(String wherekey : wherekeys){
        String fieldIndex = wherekey.split("=")[0];
        String key = wherekey.split("=")[1];
        filterKey = key + "+*+" + filterKey;
        FileSystem fs = FileSystem.get(conf);;
        Path newPath = new Path(indexfile + fieldIndex);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(newPath)));
        String line = "  ";
        while (line != null) {
          //System.out.println(line);
          line = bufferedReader.readLine();
          String[] meta = line.split(",");
          if(meta[0].equals(key)){
            Segment newSeg = segTable.get(meta[1]);
            if(newSeg == null){
              newSeg = new Segment(Long.parseLong(meta[1]), Long.parseLong(meta[2]), Long.parseLong(meta[3]));
              newSeg.addKeyword(meta[0]);
              newSeg.addFrequency(Long.parseLong(meta[4]));
              segTable.put(meta[1], newSeg);
            }
            else{
              newSeg.addKeyword(meta[0]);
              newSeg.addFrequency(Long.parseLong(meta[4]));
            }
            
          }
        }
      }
      

      if(groupBy != null){
        FileSystem fs = FileSystem.get(conf);;
        Path newPath = new Path(indexfile + groupBy);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(newPath)));
        String line = bufferedReader.readLine();
        String[] meta = line.split(",");
        String preKey = meta[0];
        while (line != null) {
          //System.out.println(line);
          meta = line.split(",");
          Segment newSeg = segTable.get(meta[1]);
          if(newSeg == null){
            newSeg = new Segment(Long.parseLong(meta[1]), Long.parseLong(meta[2]), Long.parseLong(meta[3]));
            newSeg.addKeyword(meta[0]);
            newSeg.addFrequency(Long.parseLong(meta[4]));
            segTable.put(meta[1], newSeg);
          }
          else{
            newSeg.addKeyword(meta[0]);
            newSeg.addFrequency(Long.parseLong(meta[4]));
          }
          if(! preKey.equals(meta[0])){
            filterKeys.add(filterKey + preKey);
            preKey = meta[0];
          }
          line = bufferedReader.readLine();
        }
        filterKeys.add(filterKey + meta[0]);
      }
      else{
        filterKey = filterKey.substring(0, filterKey.length()-3);
        filterKeys.add(filterKey);
      }
      return segTable.values().toArray(new Segment[segTable.size()]);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}