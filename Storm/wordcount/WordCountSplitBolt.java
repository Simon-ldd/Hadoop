package com.simon.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordCountSplitBolt extends BaseRichBolt {

    // 数据发送到下一个bolt
    private OutputCollector collector;

    // 初始化
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    // 业务逻辑
    public void execute(Tuple tuple) {
        // 1. 获取数据
        String line = tuple.getStringByField("simon");

        // 2. 切分数据
        String[] fields = line.split(" ");

        // 3. <单词， 1>发送出去下一个bolt
        for (String w: fields) {
            collector.emit(new Values(w, 1));
        }
    }

    // 声明描述
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "sum"));
    }
}
