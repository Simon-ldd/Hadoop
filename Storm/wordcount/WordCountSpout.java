package com.simon.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordCountSpout extends BaseRichSpout {

    // 定义收集器
    private SpoutOutputCollector collector;

    // 发送数据
    public void nextTuple() {
        // 1. 发送数据
        this.collector.emit(new Values("i am simon very shuai"));

        // 2. 设置延迟
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 创建收集器， 接外部的数据源
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    // 声明
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 起别名
        outputFieldsDeclarer.declare(new Fields("simon"));
    }
}
