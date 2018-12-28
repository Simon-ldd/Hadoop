package com.simon.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

public class Application {

    public static void main(String[] args) {

        // 1. 定义主题 发送到 另一个主题中 数据清洗
        String oneTopic = "t1";
        String twoTopic = "t2";

        // 2. 设置属性
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");

        // 3. 实例对象
        StreamsConfig config = new StreamsConfig(prop);

        // 4. 流计算 拓扑
        Topology buider = new Topology();

        // 5. 定义kafka组件数据源
        buider.addSource("Source", oneTopic).addProcessor("Processor", new ProcessorSupplier<byte[], byte[]>() {
            public Processor<byte[], byte[]> get() {
                return new LogProcessor();
            }
            // 从哪里来
        }, "Source")
                // 到哪里去
        .addSink("Sink", twoTopic, "Processor");

        // 6. 实例化kafkaStream
        KafkaStreams kafkaStreams = new KafkaStreams(buider, prop);
        kafkaStreams.start();
    }
}
