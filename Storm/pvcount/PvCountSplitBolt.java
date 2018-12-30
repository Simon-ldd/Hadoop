package com.simon.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PvCountSplitBolt implements IRichBolt {

    private OutputCollector collector;
    private int pvnum = 0;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        // 1. 获取数据
        String line = tuple.getStringByField("logs");

        // 2. 切分数据
        String[] fields = line.split("\t");
        String sessionId = fields[1];

        // 3. 局部累加
        if(sessionId != null) {
            // 累加
            pvnum ++;
            // 输出
            collector.emit(new Values(Thread.currentThread().getId(), pvnum));
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 声明输出字段
        outputFieldsDeclarer.declare(new Fields("threadid", "pvnum"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
