package com.simon.hdfshbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 读取数据
        String line = value.toString();

        // 2. 切分数据
        String[] fields = line.split(",");

        // 3. 封装数据
        Put put = new Put(Bytes.toBytes(fields[0]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(fields[2]));

        // 4. 输出到Reducer端
        context.write(new ImmutableBytesWritable(Bytes.toBytes(fields[0])), put);
    }
}
