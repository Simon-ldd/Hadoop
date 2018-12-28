package com.simon.kafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer1 {

    public static void main(String[] args) {
        // 1. 配置消费者属性
        Properties prop = new Properties();

        // 2. 配置属性
        // 服务器地址指定
        prop.put("bootstrap.servers", "slave1:9092"); // 可以和producer不是同一个服务器
        // 消费者组
        prop.put("group.id", "g1");
        // 是否自动确认offset
        prop.put("enable.auto.commit", "true");
        // 序列化
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 3. 实例化
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        // 4. 释放资源， 线程安全
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }));

        // 5. 订阅topic
        consumer.subscribe(Arrays.asList("test"));

        // 6. 拉取消息
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(500);

            // 遍历消息
            for(ConsumerRecord<String, String> r: records) {
                System.out.println(r.topic() + "----" + r.value());
            }
        }
    }
}
