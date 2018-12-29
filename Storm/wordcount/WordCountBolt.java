package com.simon.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

    private Map<String, Integer> map = new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    // 累加求和
    public void execute(Tuple tuple) {
        // 1. 获取数据
        String word = tuple.getStringByField("word");
        Integer sum = tuple.getIntegerByField("sum");

        // 2. 业务处理
        if (map.containsKey(word)) {
            Integer count = map.get(word);
            map.put(word, count + sum);
        } else {
            map.put(word, sum);
        }

        // 3. 打印到控制台
        System.err.println(Thread.currentThread().getId() + "单词为: " + word + "\t当前已出现次数为: " + map.get(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
