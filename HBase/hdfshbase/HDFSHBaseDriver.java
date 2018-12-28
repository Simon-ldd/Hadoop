package com.simon.hdfshbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HDFSHBaseDriver implements Tool {
    private Configuration conf = null;

    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create(conf);
    }

    public Configuration getConf() {
        return this.conf;
    }

    public int run(String[] strings) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 创建job
        Job job = Job.getInstance(conf);
        job.setJarByClass(HDFSHBaseDriver.class);

        // 2. 配置mapper
        job.setMapperClass(ReadFromHDFSMapper.class);
        job.setMapOutputValueClass(Put.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);

        // 3. 配置Reducer
        TableMapReduceUtil.initTableReducerJob("hdfshbase", WriteToHBaseReducer.class, job);

        // 4. 配置输入inputformat
        FileInputFormat.addInputPath(job, new Path("/in/"));

        // 5. 输出

        return job.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HDFSHBaseDriver(), args);
    }
}
