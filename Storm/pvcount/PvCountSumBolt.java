package com.simon.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PvCountSumBolt implements IRichBolt {

    private HashMap<Long, Integer> hashMap = new HashMap<Long, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        // 1. 获取数据
        Long threadId = tuple.getLongByField("threadid");
        Integer pvnum = tuple.getIntegerByField("pvnum");

        // 2. 创建集合 存储
        hashMap.put(threadId, pvnum);

        // 3. 累加求和
        Iterator<Integer> iterator = hashMap.values().iterator();

        // 4. 清空之前的数据
        int sumnum = 0;
        while(iterator.hasNext()) {
            sumnum += iterator.next();
        }

        System.out.println(Thread.currentThread().getName() + "\t总访问量->" + sumnum);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
