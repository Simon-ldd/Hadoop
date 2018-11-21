package com.Order.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/simon/downloads/simon/inOrder"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/simon/downloads/simon/outOrder"));

        System.out.println(job.waitForCompletion(true)? 0 : 1);
    }
}
