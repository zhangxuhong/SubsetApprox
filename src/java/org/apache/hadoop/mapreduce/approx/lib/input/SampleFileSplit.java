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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}. */
public class SampleFileSplit extends InputSplit implements Writable {
  private Path file;
  private long[] start;
  private long[] length;
  private long totLength;
  private String[] keys;
  private String[] hosts;
  private String[] weights;
  SampleFileSplit() {}

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public SampleFileSplit(Path file, long[] start, long[] length, String[] keys, String weights, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.keys = keys;
    this.hosts = hosts;
    this.weights = weights;
    this.totLength = 0;
    for(long len : length){
      totLength += len;
    }
  }
 
  /** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long[] getStart() { return start; }

  public long getOffset(int i){
    return start[i];
  }
  
  /** The number of bytes in the file to process. */
  @Override
  public long getLength() { return totLength; }

  public long getLength(int i){
    return length[i];
  }

  public String[] getKeys() {return keys;}
  public String getKeys(int i){
    return keys[i];
  }

  public String[] getWeights() {return weights;}
  public String getWeights(int i){
    return weights[i];
  }

  public int getNumSegments(){
    return start.length;
  }

  //*************************************************need modification*********************************
  @Override
  public String toString() { return file + ":" + start.length + "+" + length.length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString());
    out.writeLong(totLength);
    out.writeInt(start.length);
    for(long s : start){
      out.writeLong(s);
    }
    out.writeInt(length.length);
    for(long len : length){
      out.writeLong(len);
    }
    out.writeInt(keys.length);
    for(long key : keys){
      Text.writeString(out, key);
    }
    out.writeInt(weights.length);
    for(long weight : weights){
      Text.writeString(out, weight);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in));
    totLength = in.readLong();
    int startLength = in.readInt();
    start = new long[startLength];
    for(int i=0; i<startLength;i++) {
      start[i] = in.readLong();
    }
    int arrLength = in.readInt();
    length = new long[arrLength];
    for(int i=0; i<arrLength;i++){
      length[i] = in.readLong();
    }
    int keysLength = in.readInt();
    keys = new String[keysLength];
    for(int i=0; i<keysLength;i++){
      keys[i] = Text.readString(in);
    }
    int weightsLength = in.readInt();
    weights = new String[weightsLength];
    for(int i=0; i<weightsLength;i++){
      weights[i] = Text.readString(in);
    }
    hosts = null;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }
}
