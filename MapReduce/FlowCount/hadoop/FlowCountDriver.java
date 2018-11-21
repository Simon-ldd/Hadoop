package com.flowcount.mapreduce.hadoop.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2. 获取jar包
        job.setJarByClass(FlowCountDriver.class);

        //3. 获取自定义的mapper与reducer类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //4. 设置map输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5. 设置reduce输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //6. 设置输入路径与处理后的结果路径
        FileOutputFormat.setOutputPath(job, new Path("/Users/simon/downloads/simon/out"));
        FileInputFormat.setInputPaths(job, new Path("/Users/simon/downloads/simon/in"));

        //7. 提交任务
        System.out.println(job.waitForCompletion(true)? 0 : 1);
    }
}
