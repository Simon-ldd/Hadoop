package com.simon.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountDriver {

    public static void main(String[] args) {
        // 1. 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        // 2. 指定设置
        // 后面的数字表示并行度， fieldsGrouping设置分组策略
        builder.setSpout("WordCountSpout", new WordCountSpout(), 1);
        builder.setBolt("WordCountSplitBolt", new WordCountSplitBolt(), 4).fieldsGrouping("WordCountSpout", new Fields("simon"));
        builder.setBolt("WordCountBolt", new WordCountBolt(), 2).fieldsGrouping("WordCountSplitBolt", new Fields("word"));

        // 3. 创建配置信息
        Config conf = new Config();

        // 4. 提交任务
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcounttopology", conf, builder.createTopology());
    }
}
