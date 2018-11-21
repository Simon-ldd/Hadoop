package com.simon.ordertable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FileDriver.class);

        job.setMapperClass(FileMapper.class);
        job.setReducerClass(FileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(FuncFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/simon/downloads/simon/inlog/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/simon/downloads/simon/outlog"));

        System.out.println(job.waitForCompletion(true)? 0 : 1);
    }
}
