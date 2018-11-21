package com.wordcount.mapreduce.simon;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 * 汇总
 *
 * KEYIN: 输入reduce的k
 * VALUEIN: 输入reduce的v
 * KEYOUT: 输出reduce的k
 * VALUEOUT: 输出reduce的v
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static int num = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1. 统计单词出现次数
        int sum = 0;

        if(num < 3) {
            //2. 累加求和
            for (IntWritable count : values) {
                //拿到值累加
                sum += count.get();
            }

            //3. 结果输出
            context.write(key, new IntWritable(sum));

            num ++;
        }
    }
}
