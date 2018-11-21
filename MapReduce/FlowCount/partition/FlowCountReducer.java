package com.flowcount.mapreduce.partition.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    // 在reduce阶段reducer会把相同key的value放到一个reduce中处理

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1. 相同手机号的流量使用再次汇总
        long upFlowSum = 0;
        long dfFlowSum = 0;

        //2. 累加
        for(FlowBean f: values){
            upFlowSum += f.getUpFlow();
            dfFlowSum += f.getDfFlow();
        }

        //3. 输出
        context.write(key, new FlowBean(upFlowSum, dfFlowSum));
    }
}
