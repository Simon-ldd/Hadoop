package com.flowcount.mapreduce.partition.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonenumPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int i) {
        //1. 获取手机号前三位
        String phoneNum = key.toString().substring(0, 3);

        //2. 分区
        switch(phoneNum) {
            case "135":
                return 0;
            case "134":
                return 1;
            case "133":
                return 2;
            case "132":
                return 3;
        }

        //3. 其他情况
        return 4;
    }
}
