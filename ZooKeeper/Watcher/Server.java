package com.ZooKeeper.Watcher;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

public class Server {

    static ZooKeeper zkCli = null;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        zkCli = new ZooKeeper("10.206.39.50:2181,10.206.39.37:2181,10.206.39.45:2181", 3000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Path: " + event.getPath() + "\tType: " + event.getType() + "\tState: " + event.getState());

                try {
                    List<String> children = zkCli.getChildren("/", true);
                    for(String c: children)
                        System.out.println(c);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Thread.sleep(Long.MAX_VALUE);
        zkCli.close();
    }
}
