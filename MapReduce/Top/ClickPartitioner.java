package com.bytedance.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ClickPartitioner extends Partitioner<Text, UserBean> {

    @Override
    public int getPartition(Text key, UserBean value, int i) {
        return (key.hashCode() & Integer.MAX_VALUE ) % i;
    }
}
