package com.ZooKeeper.WatchDemo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

public class WatchDemo1 {

    static List<String> children = null;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zkCli = new ZooKeeper("10.206.39.50:2181,10.206.39.37:2181,10.206.39.45:2181", 3000, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println("正在监听中......");
            }
        });

        // 监听目录
        children = zkCli.getChildren("/", new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println("Path: " + event.getPath());
                System.out.println("State: " + event.getState());
                System.out.println("Type: " + event.getType());
                for (String c: children) {
                    System.out.println(c);
                }
            }
        }, null);

        Thread.sleep(Long.MAX_VALUE);
        zkCli.close();
    }
}
