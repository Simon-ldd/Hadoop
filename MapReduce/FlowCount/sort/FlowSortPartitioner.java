package com.flowcount.mapreduce.sort.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowSortPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean key, Text value, int i) {
        String phoneNum = value.toString().substring(0, 3);

        switch (phoneNum) {
            case "137":
                return 0;
            case "135":
                return 1;
            case "133":
                return 2;
            case "131":
                return 3;
        }

        return 4;
    }
}
