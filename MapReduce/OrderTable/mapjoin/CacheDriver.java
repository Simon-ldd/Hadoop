package com.simon.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CacheDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CacheDriver.class);

        job.setMapperClass(CacheMapper.class);

        //这里没用Reducer可以不写
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("file:///Users/simon/downloads/simon/inMapjoin/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("/Users/simon/downloads/simon/inMapjoin/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/simon/downloads/simon/outMapjoin"));

        System.out.println(job.waitForCompletion(true)? 0 : 1);
    }
}
