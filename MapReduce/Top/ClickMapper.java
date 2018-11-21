package com.bytedance.simon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ClickMapper extends Mapper<LongWritable, Text, Text, UserBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        String[] fields = value.toString().split("\t");

        if (name.equals("a.txt")) {
            UserBean user = new UserBean(fields[1], fields[0], "", "");
            context.write(new Text(fields[1]), user);
        } else {
            UserBean user = new UserBean(fields[0], "", fields[1], fields[2]);
            context.write(new Text(fields[0]), user);
        }
    }
}
