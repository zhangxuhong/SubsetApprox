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

  		public void addkey(String onekey){

  			keys = keys + "\t" + onekey;
  		}

  		public long getOffset(){
  			return offset;
  		}
  		public long getLength(){
  			return length;
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