package com.simon.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class PvCountDriver {
    public static void main(String[] args) {
        // 1. 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        // 2. 指定设置
        builder.setSpout("PvCountSpout", new PvCountSpout(), 1);
        builder.setBolt("PvCountSplitBolt", new PvCountSplitBolt(), 4).shuffleGrouping("PvCountSpout");
        builder.setBolt("PvCountSumBolt", new PvCountSumBolt(), 1).shuffleGrouping("PvCountSplitBolt");

        // 3. 创建配置信息
        Config conf = new Config();
        conf.setNumWorkers(4);

        // 4. 提交任务
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("pvcounttopology", conf, builder.createTopology());
    }
}
