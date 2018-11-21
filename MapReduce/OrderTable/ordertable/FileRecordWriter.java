package com.simon.ordertable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FileRecordWriter extends RecordWriter<Text, NullWritable> {

    private Configuration conf = null;
    private FileSystem fs = null;
    private FSDataOutputStream itstartlog = null;
    private FSDataOutputStream otherslog = null;

    //传入task的context信息，获取数据输出路径等
    public FileRecordWriter(TaskAttemptContext context) {

        //获取配置信息
        this.conf = context.getConfiguration();
        //获取文件系统
        try {
            this.fs = FileSystem.get(this.conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //获取输出路径
        try {
            this.itstartlog = fs.create(new Path("/Users/simon/downloads/simon/outlog/itstar.log"));
            this.otherslog = fs.create(new Path("/Users/simon/downloads/simon/outlog/others.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //数据输出
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断key
        if(key.toString().contains("itstaredu")) {
            //写到文件
            itstartlog.write((key.toString() + "\n").getBytes());
        } else {
            otherslog.write((key.toString() + "\n").getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭资源
        if(null != this.otherslog)
            this.otherslog.close();
        if(null != this.itstartlog)
            this.itstartlog.close();
    }
}
