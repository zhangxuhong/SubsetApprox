package org.apache.hadoop.mapreduce.approx;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import org.apache.log4j.Logger;

/**
 * A partitioner that checks if we have a parameter and send it to the specified one.
 * \0PARAMETER-1-1 -> 1
 * origkey from map 10 -> origkey10
 */
public class ApproximatePartitioner<K,V> extends HashPartitioner<K,V> {
	/**
	 * Overwrite the partitioner to send to everybody.
	 */
	private static final Logger LOG = Logger.getLogger("Subset.Partitioner");
	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		if (key instanceof Text) {
			String aux = ((Text)key).toString();
			byte[] bytes = aux.getBytes();
			// Check if it's a parameter, it has the \0 first to guarantee is the first when sorting
			int lastIndex = aux.lastIndexOf('-');
			if (aux.charAt(lastIndex+1) == 'w' && aux.length() == lastIndex + 2) {

				String originalKey = new String(bytes, 0, bytes.length-8);
				//LOG.info("w:"+ originalKey);
				return super.getPartition((K) new Text(originalKey), value, numReduceTasks);
			}
			else{
				String originalKey = new String(bytes, 0, bytes.length-4);
				//LOG.info("key:" + originalKey);
				return super.getPartition((K) new Text(originalKey), value, numReduceTasks);
			}	
		}
		// The default case shouldn't happen
		LOG.info("should not appear");
		return super.getPartition(key, value, numReduceTasks);
	}
}
