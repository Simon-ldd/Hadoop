package com.simon.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    // 释放资源
    public void close() {

    }

    // 初始化
    public void init(ProcessorContext context) {
        // 传输
        this.context = context;
    }

    // 业务逻辑
    public void process(byte[] key, byte[] value) {
        // 1. 拿到消息数据
        String message = new String(value);

        // 2. 如果包含 - 去除
        if(message.contains("-")) {
            message = message.split("-")[1];
        }

        // 3. 发送数据
        this.context.forward(key, message.getBytes());
    }
}
