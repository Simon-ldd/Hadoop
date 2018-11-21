package com.bytedance.simon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, UtimeBean, OrderBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        context.write(new UtimeBean(fields[0], fields[1].substring(0, 10)), new OrderBean(fields[1], fields[2]));
    }
}
