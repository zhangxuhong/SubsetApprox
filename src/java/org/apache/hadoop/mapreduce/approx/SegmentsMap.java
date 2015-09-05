package org.apache.hadoop.mapreduce.approx;

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

public class SegmentsMap {

	private static class Segment {
  		private long offset;
  		private long length;
  		private String keys;
      private String weights;

      public void addWeight(double weight){
        weights = weights + "*+*" + String.valueOf(weight);
      }

  		public void addKey(String onekey){

  			keys = keys + "*+*" + onekey;
  		}

  		public long getOffset(){
  			return offset;
  		}
  		public long getLength(){
  			return length;
  		}
      
      public String getKeys(){
        return keys;
      }

      public String getWeights(){
        return weights;
      }
  	}



  	public Segment[] getSampleSegmentsList(double ratio){


  		//array need to be sorted based offset.
  		return null;
  	}

  	public Segment[] getSampleSegmentsList(double error, double s2, double confidence){

  		return null;
  	}
}