package com.matrix.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MatrixMapper extends Mapper<LongWritable, Text, Text, MatrixBean> {

    private int columnB;
    private int rowB;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();

        this.columnB = Integer.parseInt(conf.get("columnB"));
        this.rowB = Integer.parseInt(conf.get("rowB"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        String name = split.getPath().getName();
`
        if (name.contains("A")) {
            for (int i = 1; i <= this.columnB; i ++)
        }
    }
}
