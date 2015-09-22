package org.apache.hadoop.mapreduce.approx;

import org.apache.hadoop.mapreduce.approx.WeightedRandomSelector;
import org.apache.hadoop.mapreduce.approx.WeightedRandomSelector.WeightedItem;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.lang.Math;
import java.lang.Comparable;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

public class SegmentsMap {

  private static final Logger LOG = Logger.getLogger("Subset.Segmap");

  private Configuration conf;
  private Path path;
  private static String FILE_PARENT="/index/table"; // e.g. "hdfs://brick0:54310/metis";
  private static String FILE_PREFIX="part-r-0000";

  public SegmentsMap (Configuration conf, Path path){
    this.conf = conf;
    this.path = path;
  }
  public static class Segment implements Comparable<Segment>{


    public int compareTo(Segment other){
      return this.offset > other.getOffset() ? 1 : -1 ;
    }

    // ***************some static info retrieved from index file.***************************
		private long offset;
		private long length;
    private long rows;
    private ArrayList<Long> frequency;
    private ArrayList<String> keyword;
    private Hashtable<String, Long> histogram;

    public Segment(long offset, long length, long rows){
      this. offset =offset;
      this.length = length;
      this.rows = rows;
      //this.frequency = new ArrayList<Long>();
      //this.keyword = new ArrayList<String>();
      this.histogram = new Hashtable<String, Long>();
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
    // public void addFrequency(long frequency){
    //   this.frequency.add(frequency);
    // }
    // public void addKeyword(String keyword){
    //   this.keyword.add(keyword);
    // }
    public void addHistogramRecord(String keyword, long frequency){
      this.histogram.put(keyword, frequency);
    }

    public int getKeyWeight(String key){
      //return (double)frequency[0]/rows;
      String[] fields = key.split(Pattern.quote("+*+"));
      double w = 1;
      for(String field : fields){
        w = w * (histogram.get(field) / (double)rows);
      }

      return (int)Math.round(rows * w);
    }
    public long getRows(){
      return rows;
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
      if(this.weights == null){
        this.weights = String.valueOf(weight);
      }
      else{
        weights = weights + "*+*" + String.valueOf(weight);
      }
    }

    public void addKey(String onekey){
      if(this.keys == null){
        this.keys = onekey;
      }
      else{
        keys = keys + "*+*" + onekey;
      }
    }

    public String getWeights(){
      return weights;
    }

    public String getKeys(){
      return keys;
    }
  		
  }



	public Segment[] getSampleSegmentsList(){

    List<Segment> sampleSegmentsList = new ArrayList<Segment>();
		//array need to be sorted based offset.
    String[] whereKeys = conf.get("map.input.where.clause", null).split(Pattern.quote(","));
    String groupBy = conf.get("map.input.groupby.clause", null);
    
    ArrayList<String> filterKeys = new ArrayList<String>();
    Segment[] keysSegments =  this.retrieveKeyHistogram(whereKeys, groupBy, filterKeys);
    List<WeightedItem<Segment>> weightedSegs = new ArrayList<WeightedItem<Segment>>(keysSegments.length);

    for(Segment seg : keysSegments){
      weightedSegs.add(new WeightedItem<Segment>(1, seg));
    }

    if(conf.getBoolean("map.input.sampling.ratio", false)){
      double ratio = Double.parseDouble(conf.get("map.input.sample.ratio.value", "0.01"));
      for(String filterKey : filterKeys){
        for(WeightedItem<Segment> seg : weightedSegs){
          seg.setWeight(seg.getItem().getKeyWeight(filterKey));
        }
        this.randomProcess(weightedSegs, sampleSegmentsList, filterKey, ratio);
      }
    }
    else{
      long sampleSize = 0;
      for(String filterKey : filterKeys){
        for(WeightedItem<Segment> seg : weightedSegs){
          seg.setWeight(seg.getItem().getKeyWeight(filterKey));
        }
        sampleSize = conf.getLong("map.input.sample.size." + filterKey, 0);
        this.randomProcess(weightedSegs, sampleSegmentsList, filterKey, sampleSize);
      }
    }
    
    Collections.sort(sampleSegmentsList);
		return sampleSegmentsList.toArray(new Segment[sampleSegmentsList.size()]);
	}


  private void randomProcess(List<WeightedItem<Segment>> weightedSegs, 
                              List<Segment> sampleSegmentsList, String key, long sampleSize) {
    WeightedRandomSelector selector = new WeightedRandomSelector(weightedSegs);
    for(int i = 0; i < sampleSize + 1;){
      WeightedItem<Segment> candidate = selector.select();
      double weight = (double)(candidate.getWeight()) / selector.getRangeSize();
      i += candidate.getWeight();
      this.addToSampleSegmentList(candidate.getItem(), sampleSegmentsList, key, weight);
    }
    
  }

  private void randomProcess(List<WeightedItem<Segment>> weightedSegs, 
                              List<Segment> sampleSegmentsList, String key, double ratio) {
    WeightedRandomSelector selector = new WeightedRandomSelector(weightedSegs);
    long records = (long)Math.ceil(selector.getRangeSize() * ratio);
    for(int i = 0; i < records + 1;){
      WeightedItem<Segment> candidate = selector.select();
      double weight = (double)(candidate.getWeight()) / selector.getRangeSize();
      i += candidate.getWeight();
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
      String indexfile = this.FILE_PARENT + "/" + tableName + "/" + this.FILE_PREFIX;
      String filterKey = "";
      for(String wherekey : wherekeys){
        String fieldIndex = wherekey.split(Pattern.quote("="))[0];
        String key = wherekey.split(Pattern.quote("="))[1];
        filterKey = key + "+*+" + filterKey;
        FileSystem fs = FileSystem.get(conf);;
        Path newPath = new Path(indexfile + fieldIndex);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(newPath)));
        String line = bufferedReader.readLine();
        while (line != null) {
          //System.out.println(line);
          
          //LOG.info(line);
          String[] meta = line.split(Pattern.quote(","));
          //LOG.info(line + meta.length);
          if(meta[0].equals(key)){
            Segment newSeg = segTable.get(meta[1]);
            if(newSeg == null){
              newSeg = new Segment(Long.parseLong(meta[1]), Long.parseLong(meta[2]), Long.parseLong(meta[3]));
              //newSeg.addKeyword(meta[0]);
              //newSeg.addFrequency(Long.parseLong(meta[4]));
              newSeg.addHistogramRecord(meta[0], Long.parseLong(meta[4]));
              segTable.put(meta[1], newSeg);
            }
            else{
              //newSeg.addKeyword(meta[0]);
              //newSeg.addFrequency(Long.parseLong(meta[4]));
              newSeg.addHistogramRecord(meta[0], Long.parseLong(meta[4]));
            }
            
          }
          line = bufferedReader.readLine();
        }
      }
      

      if(groupBy != null){
        FileSystem fs = FileSystem.get(conf);;
        Path newPath = new Path(indexfile + groupBy);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(newPath)));
        String line = bufferedReader.readLine();
        String[] meta = line.split(Pattern.quote(","));
        String preKey = meta[0];
        while (line != null) {
          //System.out.println(line);
          meta = line.split(Pattern.quote(","));
          Segment newSeg = segTable.get(meta[1]);
          if(newSeg == null){
            newSeg = new Segment(Long.parseLong(meta[1]), Long.parseLong(meta[2]), Long.parseLong(meta[3]));
            //newSeg.addKeyword(meta[0]);
            //newSeg.addFrequency(Long.parseLong(meta[4]));
            newSeg.addHistogramRecord(meta[0], Long.parseLong(meta[4]));
            segTable.put(meta[1], newSeg);
          }
          else{
            //newSeg.addKeyword(meta[0]);
            //newSeg.addFrequency(Long.parseLong(meta[4]));
            newSeg.addHistogramRecord(meta[0], Long.parseLong(meta[4]));
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