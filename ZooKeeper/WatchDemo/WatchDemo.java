package com.ZooKeeper.WatchDemo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

// 监听单节点数据
public class WatchDemo {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zkCli = new ZooKeeper("10.206.39.50:2181,10.206.39.37:2181,10.206.39.45:2181", 3000, new Watcher() {

            @Override
            public void process(WatchedEvent event) {

            }
        });

        byte[] data = zkCli.getData("/simon_test", new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println("Path: " + event.getPath());
                System.out.println("State: " + event.getState());
                System.out.println("Type: " + event.getType());
            }
        }, null);

        System.out.println(new String(data));

        zkCli.close();
    }
}
