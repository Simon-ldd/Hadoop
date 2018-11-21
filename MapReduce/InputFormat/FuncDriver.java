package com.sequencefile.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class FuncDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FuncDriver.class);

        job.setMapperClass(FuncMapper.class);
        job.setReducerClass(FuncReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置自定义读取方式，否则按照默认的TextInputFormat方式读取
        job.setInputFormatClass(FuncFileInputFormat.class);

        // 设置默认的输出方式，采用SequenceFileOutputFormat合并输出
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/simon/downloads/simon/inFunc"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/simon/downloads/simon/outFunc"));

        System.out.println(job.waitForCompletion(true)? 0 : 1);
    }
}
