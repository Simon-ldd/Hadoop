package com.ZooKeeper.Watcher;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Client {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zkCli = new ZooKeeper("10.206.39.50:2181,10.206.39.37:2181,10.206.39.45:2181", 3000, null);

        for (int i = 0; i < 5; i ++) {
            String path = zkCli.create("/simon_test", "simonhenshuai".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            System.out.println(path);
        }

        zkCli.close();
    }
}
