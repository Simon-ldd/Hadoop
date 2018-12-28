package com.simon.kafkatest;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String, String> {

    // 业务逻辑
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<String, String>(record.topic(), record.partition(), record.key(), System.currentTimeMillis() + "-" + record.value());
    }

    // 发送失败调用
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    }

    // 关闭资源
    public void close() {
    }

    // 配置信息
    public void configure(Map<String, ?> map) {
    }
}
