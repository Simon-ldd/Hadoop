package com.mapcache.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CachePartitioner extends Partitioner<Text, CacheBean> {

    @Override
    public int getPartition(Text key, CacheBean value, int i) {
        return (value.getUid().hashCode() & Integer.MAX_VALUE) % i;
    }

}
