package com.flowcount.mapreduce.sort.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSortDriver.class);

        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(FlowSortPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("/Users/simon/downloads/simon/inSortPartition"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/simon/downloads/simon/outSortPartition"));

        System.out.println(job.waitForCompletion(true)? 0 : 1);
    }
}
