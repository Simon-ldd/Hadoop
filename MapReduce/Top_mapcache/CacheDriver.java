package com.mapcache.simon;

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

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CacheBean.class);

        job.setMapperClass(CacheMapper.class);

        job.setPartitionerClass(CachePartitioner.class);

        job.setNumReduceTasks(10);
        job.setReducerClass(CacheReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CacheBean.class);

        job.setOutputKeyClass(CacheBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("file:///simon/in/ub.txt"));
        FileInputFormat.setInputPaths(job, new Path("/simon/in/url.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/simon/out"));

        System.out.println(job.waitForCompletion(true)? 0 : 1);
    }
}
