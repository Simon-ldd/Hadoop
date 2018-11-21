package com.flowcount.mapreduce.sort.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 应为map阶段的排序只能对key进行排序，所以这里把K-V的位置置换
        context.write(values.iterator().next(), key);
    }
}
