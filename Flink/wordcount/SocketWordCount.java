package com.simon.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 定义连接端口
        final int port = 3333;

        // 2. 创建执行环境对象
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. 得到套接字对象（指定：主机、端口、分隔符）
        DataStreamSource<String> text = env.socketTextStream("master", port, "\n");

        // 4. 解析数据， 统计数据-单词个数
        // 数据压平
        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                // 按照空白符进行切割
                for (String word: s.split("\\s")) {
                    // <单词， 1>
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        })
                // 按照key进行分组
        .keyBy("word")
                // 设置窗口的时间长度， 5秒一次窗口， 1秒一次计算
        .timeWindow(Time.seconds(5), Time.seconds(1))
                // 聚合， 聚合函数
        .reduce(new ReduceFunction<WordWithCount>() {
            public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                // 按照key聚合
                return new WordWithCount(a.word, a.count + b.count);
            }
        });

        // 5. 打印， 可以设置并发度
        windowCounts.print().setParallelism(1);

        // 6. 执行程序
        env.execute("Socket window WordCount");
    }

    // 解析数据封装类
    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount(){

        }

        public WordWithCount(String word, long count) {
            this.count = count;
            this.word = word;
        }

        public String toString() {
            return word + ": " + count;
        }
    }
}
