/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.approx.lib.input;

import java.io.*;
import java.lang.reflect.*;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.hadoop.mapreduce.approx.lib.input.SampleFileSplit;
/**
 * A generic RecordReader that can hand out different recordReaders
 * for each chunk in a {@link SampleFileSplit}.
 * A SampleFileSplit can combine data chunks from multiple files. 
 * This class allows using different RecordReaders for processing
 * these data chunks from different files.
 * @see SampleFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SampleRecordReader<K, V> extends RecordReader<K, V> {
  private static final Logger LOG = Logger.getLogger("Subset.Reader");
  static final Class [] constructorSignature = new Class [] 
                                         {SampleFileSplit.class,
                                          TaskAttemptContext.class,
                                          Integer.class};

  protected SampleFileSplit split;
  protected Class<? extends RecordReader<K,V>> rrClass;
  protected Constructor<? extends RecordReader<K,V>> rrConstructor;
  protected FileSystem fs;
  protected TaskAttemptContext context;
  
  protected int idx;
  protected long progress;
  protected RecordReader<K, V> curReader;
  protected String[] whereKeys;
  protected JSONParser parser;
  
  public void initialize(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    this.split = (SampleFileSplit)split;
    this.context = context;
    whereKeys = context.getConfiguration().get("map.input.where.clause", null).split(Pattern.quote(","));
    parser = new JSONParser();
    if (null != this.curReader) {
      this.curReader.initialize(split, context);
    }
  }
  

  public boolean nextKeyValue() throws IOException, InterruptedException {
    while(nextKeyValueOrg()){
      String currentValue = ((Text)this.getCurrentValue()).toString();
      JSONObject jsonCurrentValue = null;
      try{
        jsonCurrentValue = (JSONObject)parser.parse(currentValue);
      } catch (ParseException e){
        e.printStackTrace();
      }
      //LOG.info(currentValue);
      String[] keys = this.split.getKeys(idx-1).split(Pattern.quote("*+*"));
      for(String key : keys){
      	//*************************************************************************fields separator**********************
        String[] fields = key.split(Pattern.quote("+*+"));
        boolean containCurrentKey = true;

        for(int i = 0; i < fields.length; i++){
          //LOG.info("filter:" + field);
          String[] jsonwhere = whereKeys[i].split(Pattern.quote("."));
          JSONObject jsonkey = jsonCurrentValue;
          for(int j = 0; j < jsonwhere.length; j++){
            if(jsonkey.containsKey(jsonwhere[j])){
              jsonkey = (JSONObject)jsonkey.get(jsonwhere[j]);
            }else{
              containCurrentKey = false;
              break;
            }
          }
          if(!containCurrentKey){
            break;
          }
          String jsonvalue = jsonkey.toString();
          if(!jsonvalue.toLowerCase().contains(fields[i].toLowerCase())){
            containCurrentKey = false;
            break;
          }
        }
        if(containCurrentKey){
          return true;
        }
      } 
    }
    return false;
  }

  public boolean nextKeyValueOrg() throws IOException, InterruptedException {

    while ((curReader == null) || !curReader.nextKeyValue()) {
      if (!initNextRecordReader()) {
        //LOG.info("next:false");
        return false;
      }
    }
    //LOG.info("next:true");
    return true;
  }

  public K getCurrentKey() throws IOException, InterruptedException {
    return curReader.getCurrentKey();
  }
  
  public V getCurrentValue() throws IOException, InterruptedException {
    
    //currentValue = new Text(currentValue.toString() + "+" + String.valueOf(idx-1));
    return curReader.getCurrentValue();
  }
  

  public int getCurrentClusterID(){
  	return idx-1;
  }

  public String getCurrentSegKeys(int segid){
    return split.getKeys(segid);
  }

  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }
  
  /**
   * return progress based on the amount of data processed so far.
   */
  public float getProgress() throws IOException, InterruptedException {
    long subprogress = 0;    // bytes processed in current split
    if (null != curReader) {
      // idx is always one past the current subsplit's true index.
      subprogress = (long)(curReader.getProgress() * split.getLength(idx - 1));
    }
    return Math.min(1.0f,  (progress + subprogress)/(float)(split.getLength()));
  }
  
  /**
   * A generic RecordReader that can hand out different recordReaders
   * for each chunk in the SampleFileSplit.
   */
  public SampleRecordReader(SampleFileSplit split,
                                 TaskAttemptContext context,
                                 Class<? extends RecordReader<K,V>> rrClass)
    throws IOException {
    this.split = split;
    this.context = context;
    this.rrClass = rrClass;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;

    try {
      rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
      rrConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(rrClass.getName() + 
                                 " does not have valid constructor", e);
    }
    initNextRecordReader();
  }
  
  /**
   * Get the record reader for the next chunk in this SampleFileSplit.
   */
  protected boolean initNextRecordReader() throws IOException {

    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progress += split.getLength(idx-1);    // done processing so far
      }
    }

    // if all chunks have been processed, nothing more to do.
    if (idx == split.getNumSegments()) {
      return false;
    }

    // get a record reader for the idx-th chunk
    try {
      Configuration conf = context.getConfiguration();
      // setup some helper config variables.
      conf.set("map.input.file", split.getPath().toString());
      conf.setLong("map.input.start", split.getOffset(idx));
      conf.setLong("map.input.length", split.getLength(idx));

      curReader =  rrConstructor.newInstance(new Object [] 
                            {split, context, Integer.valueOf(idx)});

      if (idx > 0) {
        // initialize() for the first RecordReader will be called by MapTask;
        // we're responsible for initializing subsequent RecordReaders.
        curReader.initialize(split, context);
      }
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    idx++;
    return true;
  }
}
