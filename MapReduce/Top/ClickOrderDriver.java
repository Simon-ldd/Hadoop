package com.bytedance.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ClickOrderDriver {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();

        Job job1 = Job.getInstance(conf1);
        Job job2 = Job.getInstance(conf2);

        job1.setJarByClass(ClickOrderDriver.class);
        job2.setJarByClass(ClickOrderDriver.class);

        job1.setMapperClass(ClickMapper.class);
        job1.setPartitionerClass(ClickPartitioner.class);
        job1.setReducerClass(ClickReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(UserBean.class);

        job1.setOutputKeyClass(UserBean.class);
        job1.setOutputValueClass(NullWritable.class);

        job1.setNumReduceTasks(10);

        FileInputFormat.setInputPaths(job1, new Path("/simon/in"));
        FileOutputFormat.setOutputPath(job1, new Path("/simon/mout"));

        job2.setMapperClass(OrderMapper.class);
        job2.setPartitionerClass(OrderPartitioner.class);
        job2.setReducerClass(OrderReducer.class);

        job2.setMapOutputKeyClass(UtimeBean.class);
        job2.setMapOutputValueClass(OrderBean.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(OrderBean.class);

        job2.setNumReduceTasks(10);

        FileInputFormat.setInputPaths(job2, new Path("/simon/mout"));
        FileOutputFormat.setOutputPath(job2, new Path("/simon/out"));

        JobControl control = new JobControl("ClickOrder");

        ControlledJob aJob = new ControlledJob(job1.getConfiguration());
        ControlledJob bJob = new ControlledJob(job2.getConfiguration());

        bJob.addDependingJob(aJob);

        control.addJob(aJob);
        control.addJob(bJob);

        Thread thread = new Thread(control);
        thread.start();

        while(!control.allFinished()){
            thread.sleep(1000);
        }
        System.exit(0);
    }
}
