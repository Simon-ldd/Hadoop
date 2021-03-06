package com.simon.kafkatest;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

public class Producer3 {

    public static void main(String[] args) {

        // 1. 配置生产者属性(指定多个参数)
        Properties prop = new Properties();
        // kafka节点地址
        prop.put("bootstrap.servers", "master:9092");
        // 发送消息是否等待应答
        prop.put("acks", "all");
        // 配置发送消息失败重发
        prop.put("retries", "0");
        // 批量处理消息大小
        prop.put("batch.size", "10241");
        // 批量处理数据延迟
        prop.put("linger.ms", "5");
        // 内存缓冲大小
        prop.put("buffer.memory", "12341235");
        // 消息在发送前必须序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 拦截器
        ArrayList<String> inList = new ArrayList<String>();
        inList.add("com.simon.kafkatest.TimeInterceptor");
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, inList);

        // 2. 实例化producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        // 3. 发送消息
        for (int i = 0; i < 100; i ++) {
            producer.send(new ProducerRecord<String, String>("test", "simonnb " + i), new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 如果metadata不为null，拿到当前的偏移量与分区
                    if (metadata != null) {
                        System.out.println(metadata.topic() + "----" + metadata.offset() + "----" + metadata.partition());
                    }
                }
            });
        }

        //4. 释放资源
        producer.close();
    }
}
