package com.sequencefile.simon;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


/**
 * FileInputFormat<K, V>
 * K表示输入路径，V表示文件具体数据
 */
public class FuncFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // 不切原来的文件
        return false;
    }

    // 通过源码可以知道实际上是通过RecorReader来读取数据，这里我们想要自定义读取方式，就一定眼自己写一个RecordReader
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FuncRecordReader recordReader = new FuncRecordReader();
        return recordReader;
    }
}
