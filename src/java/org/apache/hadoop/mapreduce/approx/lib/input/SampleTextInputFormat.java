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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.approx.SegmentsMap;
import org.apache.hadoop.mapreduce.approx.SegmentsMap.Segment;
import org.apache.log4j.Logger;

public abstract class SampleTextInputFormat<K, V> extends FileInputFormat<K, V>{
  private static final Logger LOG = Logger.getLogger("Subset.InputFormat");
  public static final String SPLIT_MINSIZE_PERNODE = 
    "mapreduce.input.fileinputformat.split.minsize.per.node";
  public static final String SPLIT_MINSIZE_PERRACK = 
    "mapreduce.input.fileinputformat.split.minsize.per.rack";
  // ability to limit the size of a single split
  private long maxSplitSize = 0;
  private long minSplitSizeNode = 0;
  private long minSplitSizeRack = 0;
  private boolean blockunit = false;
  // mapping from a rack name to the set of Nodes in the rack 
  private HashMap<String, Set<String>> rackToNodes = 
                            new HashMap<String, Set<String>>();
  
  public SampleTextInputFormat() {
  }



  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  /**
   * Specify the minimum size (in bytes) of each split per node.
   * This applies to data that is left over after combining data on a single
   * node into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeNode.
   */
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  /**
   * Specify the minimum size (in bytes) of each split per rack.
   * This applies to data that is left over after combining data on a single
   * rack into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeRack.
   */
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }


  @Override
  public List<InputSplit> getSplits(JobContext job) 
    throws IOException {

    long minSizeNode = 0;
    long minSizeRack = 0;
    long maxSize = 0;
    Configuration conf = job.getConfiguration();
    blockunit = conf.getBoolean("map.input.block.unit", false);

    // the values specified by setxxxSplitSize() takes precedence over the
    // values that might have been specified in the config
    if (minSplitSizeNode != 0) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = conf.getLong(SPLIT_MINSIZE_PERNODE, 0);
    }
    if (minSplitSizeRack != 0) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = conf.getLong(SPLIT_MINSIZE_PERRACK, 0);
    }
    if (maxSplitSize != 0) {
      maxSize = maxSplitSize;
    } else {
      maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 67108864);
    }
    if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
      throw new IOException("Minimum split size pernode " + minSizeNode +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
      throw new IOException("Minimum split size per rack" + minSizeRack +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && minSizeNode > minSizeRack) {
      throw new IOException("Minimum split size per node" + minSizeNode +
                            " cannot be smaller than minimum split " +
                            "size per rack " + minSizeRack);
    }

    // all the files in input set

    // all the files in input set
    Path[] paths = FileUtil.stat2Paths(
                     listStatus(job).toArray(new FileStatus[0]));
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (paths.length == 0) {
      return splits;    
    }

    // Convert them to Paths first. This is a costly operation and 
    // we should do it first, otherwise we will incur doing it multiple
    // times, one time each for each pool in the next loop.
    List<Path> newpaths = new LinkedList<Path>();
    for (int i = 0; i < paths.length; i++) {
      FileSystem fs = paths[i].getFileSystem(conf);
      Path p = fs.makeQualified(paths[i]);
      newpaths.add(p);
    }
    paths = null;

    // create splits for all files that are not in any pool.
    getMoreSplits(job, newpaths.toArray(new Path[newpaths.size()]), 
                  maxSize, minSizeNode, minSizeRack, splits);

    // free up rackToNodes map
    rackToNodes.clear();
    return splits;    
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplits(JobContext job, Path[] paths, 
                             long maxSize, long minSizeNode, long minSizeRack,
                             List<InputSplit> splits)
    throws IOException {
    Configuration conf = job.getConfiguration();

    // all blocks for all the files in input set
    OneFileInfo[] files;
  
    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks = 
                              new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes = 
                              new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, List<OneBlockInfo>> nodeToBlocks = 
                              new HashMap<String, List<OneBlockInfo>>();
    
    files = new OneFileInfo[paths.length];
    if (paths.length == 0) {
      return; 
    }

    // populate all the blocks for all files
    //***************************************sampling info*************************************
    //long totLength = 0;
    for (int i = 0; i < paths.length; i++) {
      files[i] = new OneFileInfo(paths[i], conf, isSplitable(job, paths[i]),
                                 rackToBlocks, blockToNodes, nodeToBlocks,
                                 rackToNodes, maxSize);
      //totLength += files[i].getLength();
    }

    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    Set<String> nodes = new HashSet<String>();
    long curSplitSize = 0;

    // process all nodes and create splits that are local
    // to a node. 
    for (Iterator<Map.Entry<String, 
         List<OneBlockInfo>>> iter = nodeToBlocks.entrySet().iterator(); 
         iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> one = iter.next();
      nodes.add(one.getKey());
      List<OneBlockInfo> blocksInNode = one.getValue();

      // for each block, copy it into validBlocks. Delete it from 
      // blockToNodes so that the same block does not appear in 
      // two different splits.
      for (OneBlockInfo oneblock : blocksInNode) {
        if (blockToNodes.containsKey(oneblock)) {
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          //*******************************************segments compose splits****************
          curSplitSize += oneblock.length;
          if(blockunit){
            addCreatedSplit1(splits, validBlocks);
            curSplitSize = 0;
            validBlocks.clear();
            continue;
          }
          // if the accumulated split size exceeds the maximum, then 
          // create this split.
          if (maxSize != 0 && curSplitSize >= maxSize) {
            // create an input split and add it to the splits array
            addCreatedSplit(splits, nodes, validBlocks);
            curSplitSize = 0;
            validBlocks.clear();
          }
        }
      }
      // if there were any blocks left over and their combined size is
      // larger than minSplitNode, then combine them into one split.
      // Otherwise add them back to the unprocessed pool. It is likely 
      // that they will be combined with other blocks from the 
      // same rack later on.
      if (minSizeNode != 0 && curSplitSize >= minSizeNode) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits, nodes, validBlocks);
      } else {
        for (OneBlockInfo oneblock : validBlocks) {
          blockToNodes.put(oneblock, oneblock.hosts);
        }
      }
      validBlocks.clear();
      nodes.clear();
      curSplitSize = 0;
    }

    // if blocks in a rack are below the specified minimum size, then keep them
    // in 'overflow'. After the processing of all racks is complete, these 
    // overflow blocks will be combined into splits.
    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    Set<String> racks = new HashSet<String>();

    // Process all racks over and over again until there is no more work to do.
    while (blockToNodes.size() > 0) {

      // Create one split for this rack before moving over to the next rack. 
      // Come back to this rack after creating a single split for each of the 
      // remaining racks.
      // Process one rack location at a time, Combine all possible blocks that
      // reside on this rack as one split. (constrained by minimum and maximum
      // split size).

      // iterate over all racks 
      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = 
           rackToBlocks.entrySet().iterator(); iter.hasNext();) {

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        racks.add(one.getKey());
        List<OneBlockInfo> blocks = one.getValue();

        // for each block, copy it into validBlocks. Delete it from 
        // blockToNodes so that the same block does not appear in 
        // two different splits.
        boolean createdSplit = false;
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {
            validBlocks.add(oneblock);
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;
      
            // if the accumulated split size exceeds the maximum, then 
            // create this split.
            if (maxSize != 0 && curSplitSize >= maxSize) {
              // create an input split and add it to the splits array
              addCreatedSplit(splits, getHosts(racks), validBlocks);
              createdSplit = true;
              break;
            }
          }
        }

        // if we created a split, then just go to the next rack
        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            // if there is a minimum size specified, then create a single split
            // otherwise, store these blocks into overflow data structure
            addCreatedSplit(splits, getHosts(racks), validBlocks);
          } else {
            // There were a few blocks in this rack that 
        	// remained to be processed. Keep them in 'overflow' block list. 
        	// These will be combined later.
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    // Process all overflow blocks
    for (OneBlockInfo oneblock : overflowBlocks) {
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      // This might cause an exiting rack location to be re-added,
      // but it should be ok.
      for (int i = 0; i < oneblock.racks.length; i++) {
        racks.add(oneblock.racks[i]);
      }

      // if the accumulated split size exceeds the maximum, then 
      // create this split.
      if (maxSize != 0 && curSplitSize >= maxSize) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits, getHosts(racks), validBlocks);
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    // Process any remaining blocks, if any.
    if (!validBlocks.isEmpty()) {
      addCreatedSplit(splits, getHosts(racks), validBlocks);
    }
  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into splitList.
   */

  private void addCreatedSplit1(List<InputSplit> splitList,  
                               ArrayList<OneBlockInfo> validBlocks){
    OneBlockInfo oneblockinfo = validBlocks.get(0);
    SampleFileSplit thissplit = new SampleFileSplit(oneblockinfo.onepath, oneblockinfo.segOffset, 
                                   oneblockinfo.segLength, 
                                   oneblockinfo.segKeys, 
                                   oneblockinfo.segWeights,
                                   oneblockinfo.hosts);
    splitList.add(thissplit); 
    int index = splitList.size()-1;
    for(int i = 0; i < oneblockinfo.segOffset.length; i++){
      LOG.info("split:"+ String.valueOf(index) + " segment:" 
        + String.valueOf(i) + " offset:" + String.valueOf(oneblockinfo.segOffset[i]) 
        + " length:" + String.valueOf(oneblockinfo.segLength[i])
        + " key:" + oneblockinfo.segKeys[i]);
    }
  }



  private void addCreatedSplit(List<InputSplit> splitList, 
                               Collection<String> locations, 
                               ArrayList<OneBlockInfo> validBlocks) {
    // create an input split
    Path fl = validBlocks.get(0).onepath;
    long[] offset = null;
    long[] length = null;
    String[] key = null;
    String[] weight = null;
    for (int i = 0; i < validBlocks.size(); i++) {
      //fl[i] = validBlocks.get(i).onepath; 
      offset = ArrayUtils.addAll(offset, validBlocks.get(i).segOffset);
      length = ArrayUtils.addAll(length, validBlocks.get(i).segLength);
      key = ArrayUtils.addAll(key, validBlocks.get(i).segKeys);
      weight = ArrayUtils.addAll(weight, validBlocks.get(i).segWeights);
    }


     // add this split to the list that is returned
    SampleFileSplit thissplit = new SampleFileSplit(fl, offset, 
                                   length, 
                                   key, 
                                   weight,
                                   locations.toArray(new String[0]));
    splitList.add(thissplit); 
    int index = splitList.size()-1;
    for(int i = 0; i < offset.length; i++){
      LOG.info("split:"+ String.valueOf(index) + " segment:" 
        + String.valueOf(i) + " offset:" + String.valueOf(offset[i]) 
        + " length:" + String.valueOf(length[i])
        + " key:" + key[i]);
    }
  }

  /**
   * This is not implemented yet. 
   */
  public abstract RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException;

  /**
   * information about one file from the File System
   */




  //sampling info add***********************************************************************
  private static class OneFileInfo {
    private long fileSize;               // size of the file
    private OneBlockInfo[] blocks;       // all blocks in this file

    OneFileInfo(Path path, Configuration conf,
                boolean isSplitable,
                HashMap<String, List<OneBlockInfo>> rackToBlocks,
                HashMap<OneBlockInfo, String[]> blockToNodes,
                HashMap<String, List<OneBlockInfo>> nodeToBlocks,
                HashMap<String, Set<String>> rackToNodes,
                long maxSize)
                throws IOException {
      this.fileSize = 0;

      // get block locations from file system
      FileSystem fs = path.getFileSystem(conf);
      FileStatus stat = fs.getFileStatus(path);
      BlockLocation[] locations = fs.getFileBlockLocations(stat, 0, 
                                                           stat.getLen());
			// get all sample segments

      SegmentsMap smap = new SegmentsMap(conf, path);
      Segment[] sampleSegList = smap.getSampleSegmentsList();
      LOG.info("sampled segments:" + String.valueOf(sampleSegList.length));
      // create a list of all block and their locations
      if (locations == null) {
        blocks = new OneBlockInfo[0];
      } else {
        if (!isSplitable) {
          // if the file is not splitable, just create the one block with
          // full file length
          //blocks = new OneBlockInfo[1];
          //fileSize = stat.getLen();
          //blocks[0] = new OneBlockInfo(path, 0, fileSize, locations[0]
              //.getHosts(), locations[0].getTopologyPaths());
        } else {
          ArrayList<OneBlockInfo> blocksList = new ArrayList<OneBlockInfo>(
              locations.length);
          for (int i = 0, j= 0; i < locations.length; i++) {
            fileSize += locations[i].getLength();
            //**************************segments to block*************************************
            // each split can be a maximum of maxSize
            long blklength = locations[i].getLength();
            long blkOffset = locations[i].getOffset();
            long[] segmentOffsets = null;
            long[] segmentLengths = null;
            ArrayList<String> segmentKeys = new ArrayList<String>();
            ArrayList<String> segmentWeights = new ArrayList<String>();
            int k = j;
            while(j < sampleSegList.length && sampleSegList[j].getOffset() >= blkOffset && sampleSegList[j].getOffset() < blkOffset + blklength){
            	segmentOffsets = ArrayUtils.addAll(segmentOffsets ,new long[]{sampleSegList[j].getOffset()});
            	segmentLengths = ArrayUtils.addAll(segmentLengths ,new long[]{sampleSegList[j].getLength()});
              segmentKeys.add(sampleSegList[j].getKeys());
              segmentWeights.add(sampleSegList[j].getWeights());
            	j++;
            }
            if(j == k){
              continue;
            }
          	long[] myOffset = segmentOffsets;
          	long[] myLength = segmentLengths;
            String[] mykey = segmentKeys.toArray(new String[segmentKeys.size()]);
            String[] myweight = segmentWeights.toArray(new String[segmentWeights.size()]);

            //******************************************add segment info*************************
            OneBlockInfo oneblock = new OneBlockInfo(path, myOffset,
                myLength, mykey, myweight, locations[i].getHosts(), locations[i]
                    .getTopologyPaths());
            //left -= myLength;
            //myOffset += myLength;

            blocksList.add(oneblock);
          }
          blocks = blocksList.toArray(new OneBlockInfo[blocksList.size()]);
        }

        for (OneBlockInfo oneblock : blocks) {
          // add this block to the block --> node locations map
          blockToNodes.put(oneblock, oneblock.hosts);

          // For blocks that do not have host/rack information,
          // assign to default  rack.
          String[] racks = null;
          if (oneblock.hosts.length == 0) {
            racks = new String[]{NetworkTopology.DEFAULT_RACK};
          } else {
            racks = oneblock.racks;
          }

          // add this block to the rack --> block map
          for (int j = 0; j < racks.length; j++) {
            String rack = racks[j];
            List<OneBlockInfo> blklist = rackToBlocks.get(rack);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              rackToBlocks.put(rack, blklist);
            }
            blklist.add(oneblock);
            if (!racks[j].equals(NetworkTopology.DEFAULT_RACK)) {
              // Add this host to rackToNodes map
              addHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
            }
          }

          // add this block to the node --> block map
          for (int j = 0; j < oneblock.hosts.length; j++) {
            String node = oneblock.hosts[j];
            List<OneBlockInfo> blklist = nodeToBlocks.get(node);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              nodeToBlocks.put(node, blklist);
            }
            blklist.add(oneblock);
          }
        }
      }
    }

    long getLength() {
      return fileSize;
    }

    OneBlockInfo[] getBlocks() {
      return blocks;
    }
  }

  /**
   * information about one block from the File System
   */
  private static class OneBlockInfo {
    Path onepath;                // name of this file
    long[] segOffset;                 // offset in file
    long[] segLength;
    String[] segWeights;
    String[] segKeys;
    long length;                 // length of this block
    String[] hosts;              // nodes on which this block resides
    String[] racks;              // network topology of hosts

    OneBlockInfo(Path path, long[] offset, long[] len, String[] keys, String[] weights,
                 String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.segOffset = offset;
      this.hosts = hosts;
      this.segLength = len;
      this.segWeights = weights;
      this.segKeys = keys;
      this.length = 0;
      for(long onelen : len){
      	this.length += onelen;
      }
      assert (hosts.length == topologyPaths.length ||
              topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i], 
                              NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      // The topology paths have the host name included as the last 
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

  protected BlockLocation[] getFileBlockLocations(
    FileSystem fs, FileStatus stat) throws IOException {
    return fs.getFileBlockLocations(stat, 0, stat.getLen());
  }

  private static void addHostToRack(HashMap<String, Set<String>> rackToNodes,
                                    String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }
  
  private Set<String> getHosts(Set<String> racks) {
    Set<String> hosts = new HashSet<String>();
    for (String rack : racks) {
      if (rackToNodes.containsKey(rack)) {
        hosts.addAll(rackToNodes.get(rack));
      }
    }
    return hosts;
  }
}