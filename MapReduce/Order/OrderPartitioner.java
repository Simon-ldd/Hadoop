package com.Order.simon;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean key, NullWritable value, int i) {
        return (key.getOrderId() & Integer.MAX_VALUE) % i;
    }
}
