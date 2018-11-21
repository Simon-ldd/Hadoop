package com.bytedance.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<Text, OrderBean> {

    @Override
    public int getPartition(Text key, OrderBean value, int i) {
        return (key.hashCode() & Integer.MAX_VALUE) % i;
    }
}
