package org.apache.hadoop.mapreduce.approx;

import java.util.Random;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.lang.Comparable;

public class WeightedRandomSelector<T> {
    private final Random rnd = new Random();
    private final TreeMap<Object, Range<T>> ranges = new TreeMap<Object, Range<T>>();
    private int rangeSize; // Lowest integer higher than the top of the highest range.

    public WeightedRandomSelector(List<WeightedItem<T>> weightedItems) {
        int bottom = 0; // Increments by size of non zero range added as ranges grows.

        for(WeightedItem<T> wi : weightedItems) {
            int weight = wi.getWeight();
            if(weight > 0) {
                int top = bottom + weight - 1;
                Range<T> r = new Range<T>(bottom, top, wi);
                if(ranges.containsKey(r)) {
                    Range<T> other = ranges.get(r);
                    throw new IllegalArgumentException(String.format("Range %s conflicts with range %s", r, other));
                }
                ranges.put(r, r);
                bottom = top + 1;
            }
        }
        rangeSize = bottom; 
    }

    public WeightedItem<T> select() {
        Integer key = rnd.nextInt(rangeSize);
        Range<T> r = ranges.get(key);
        if(r == null)
            return null;
        return r.getWeightedItem();
    }

    public int getRangeSize(){
        return rangeSize;
    }

    public static class Range<T> implements Comparable<Object>{
        final int bottom;
        final int top;
        final WeightedItem<T> weightedItem;
        public Range(int bottom, int top, WeightedItem<T> wi) {
            this.bottom = bottom;
            this.top = top;
            this.weightedItem = wi;
        }

        public WeightedItem<T> getWeightedItem() {
            return weightedItem;
        }

        @Override
        public int compareTo(Object arg0) {
            if(arg0 instanceof Range<?>) {
                Range<?> other = (Range<?>) arg0;
                if(this.bottom > other.top)
                    return 1;
                if(this.top < other.bottom)
                    return -1;
                return 0; // overlapping ranges are considered equal.
            } else if (arg0 instanceof Integer) {
                Integer other = (Integer) arg0;
                if(this.bottom > other.intValue())
                    return 1;
                if(this.top < other.intValue())
                    return -1;
                return 0;
            }
            throw new IllegalArgumentException(String.format("Cannot compare Range objects to %s objects.", arg0.getClass().getName()));
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{\"_class\": Range {\"bottom\":\"").append(bottom).append("\", \"top\":\"").append(top)
                    .append("\", \"weightedItem\":\"").append(weightedItem).append("}");
            return builder.toString();
        }
    }

    public static class WeightedItem<T>{
        private int weight;
        private T item;
        public WeightedItem(int weight, T item) {
            this.item = item;
            this.weight = weight;
        }

        public T getItem() {
            return item;
        }

        public int getWeight() {
            return weight;
        }
        public void setWeight(int weight){
            this.weight = weight;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{\"_class\": WeightedItem {\"weight\":\"").append(weight).append("\", \"item\":\"")
                    .append(item).append("}");
            return builder.toString();
        }
    }


}