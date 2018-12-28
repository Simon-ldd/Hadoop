package com.simon.kafkatest;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class Partition1 implements Partitioner {

    // 设置
    public void configure(Map<String, ?> configs) {

    }

//    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
//        return 0;
//    }

    // 分区逻辑
    public int partition(String topic, Object key, byte[] keybytes, Object value, byte[] bytes1, Cluster cluster) {
        return 1;
    }

    // 释放资源
    public void close(){

    }
}
