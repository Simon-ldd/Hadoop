package com.flowcount.mapreduce.sort.simon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获得一行数据
        String line = value.toString();
        //切割
        String[] fields = line.split("\t");
        //取关键字段
        long upFlow = Long.parseLong(fields[1]);
        long dfFlow = Long.parseLong(fields[2]);
        //写出到reduce阶段
        context.write(new FlowBean(upFlow, dfFlow), new Text(fields[0]));
    }
}
