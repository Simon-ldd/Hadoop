package com.sequencefile.simon;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FuncMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    Text k = new Text();

    // 在初始化过程中先做一些处理
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件的路径，先拿到切片信息
        FileSplit split = (FileSplit)context.getInputSplit();
        // 获取路径
        Path path = split.getPath();
        //设置路径
        k.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // 输出
        context.write(k, value);
    }
}
