package com.simon.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //  区分两张表
        FileSplit split = (FileSplit)context.getInputSplit();

        TableBean v = new TableBean();
        Text k = new Text();

        String name = split.getPath().getName();
        // 获取数据
        String line = value.toString();

        // 区分
        if(name.contains("order.txt")) {
            // 切分字段
            String[] fields = line.split("\t");
            // 封装
            v.setOrderId(fields[0]);
            v.setpId(fields[1]);
            v.setpMount(Integer.parseInt(fields[2]));
            v.setpName("");
            v.setFlag("0");
            k.set(fields[1]);
        } else {
            String[] fields = line.split("\t");
            v.setOrderId("");
            v.setpId(fields[0]);
            v.setpMount(0);
            v.setpName(fields[1]);
            v.setFlag("1");
            k.set(fields[0]);
        }

        context.write(k, v);
    }
}
