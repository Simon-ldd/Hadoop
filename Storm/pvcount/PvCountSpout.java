package com.simon.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class PvCountSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader br;
    private String line;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        // 读取文件
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream("/users/simon/hadoop/storm/pvcount/weblog.log")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        // 发送读取的数据的每一行
        try {
            while((line = this.br.readLine()) != null) {
                // 发送数据到splitbolt
                collector.emit(new Values(line));

                Thread.sleep(500);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("logs"));
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
