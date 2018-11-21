package com.wordcount.mapreduce.simon;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce分为两个阶段：map、reduce
 * 这里先创建map阶段的类
 *
 * KEYIN: 数据的起始偏移量
 * VALUEIN: 输入mapper的数据
 * KEYOUT: mapper输出到reduce阶段k的类型
 * VALUEOUT: mapper输出到reduce阶段v的类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //key 起始偏移量，value 输入的数据，context 上下文的数据传输
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 读取数据
        String line = value.toString();

        //2. 切割
        String[] words = line.split(" ");

        //3. 循环写到reduce阶段
        for(String w: words){
            //4. 输出到reduce阶段
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
