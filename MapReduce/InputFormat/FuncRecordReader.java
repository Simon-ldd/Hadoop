package com.sequencefile.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RecordReader类
 */
public class FuncRecordReader extends RecordReader<NullWritable, BytesWritable> {

    boolean isProcess = true;
    FileSplit split;
    Configuration conf;
    BytesWritable value = new BytesWritable();

    // 初始化
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        //初始化文件切片
        this.split = (FileSplit)split;

        //初始化配置信息
        this.conf = context.getConfiguration();
    }

    // 读取数据的方式
    @Override
    public boolean nextKeyValue() {
        if(isProcess){
            // 根据切片长度创建缓冲区
            byte[] buf = new byte[(int)this.split.getLength()];
            // 输入流
            FSDataInputStream fis = null;
            // 文件系统
            FileSystem fs = null;

            try {
                // 获取路径
                Path path = this.split.getPath();
                // 根据路径获取文件系统
                fs = path.getFileSystem(this.conf);
                // 拿到输入流
                fis = fs.open(path);
                // 数据拷贝     输入流，缓冲区，偏移量，读取长度
                IOUtils.readFully(fis, buf, 0, buf.length);
                // 拷贝缓冲到最终输出
                this.value.set(buf, 0, buf.length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
                IOUtils.closeStream(fs);
            }

            this.isProcess = false;
            return true;
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
