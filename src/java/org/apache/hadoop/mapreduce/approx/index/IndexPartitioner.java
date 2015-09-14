package org.apache.hadoop.mapreduce.approx.index;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * A partitioner that checks if we have a parameter and send it to the specified one.
 * \0PARAMETER-1-1 -> 1
 * origkey from map 10 -> origkey10
 */
public class IndexPartitioner<K,V> extends HashPartitioner<K,V> {
	/**
	 * Overwrite the partitioner to send to everybody.
	 */
	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		if (key instanceof Text) {
			String aux = ((Text)key).toString();
			
			// Check if it's a parameter, it has the \0 first to guarantee is the first when sorting
			int lastIndex = aux.lastIndexOf('--');
			if (lastIndex > 0) {
				return Integer.parseInt(aux.substring(lastIndex+2, aux.length())) % numReduceTasks;
			}
		}
		// The default case shouldn't happen
		return super.getPartition(key, value, numReduceTasks);
	}
}
