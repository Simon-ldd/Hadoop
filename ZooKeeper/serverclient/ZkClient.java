package com.ZooKeeper.serverclient;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// 客户端
public class ZkClient {

    private ZooKeeper zkCli = null;
    private String parentNode = "/servers";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // 1. 连接集群
        ZkClient zkClient = new ZkClient();
        zkClient.getConnect();

        // 2. 监听
//        zkClient.getServers();

        // 3. 业务逻辑
        zkClient.getWatch();
    }

    // 1. 连接集群
    public void getConnect() throws IOException {
        zkCli = new ZooKeeper("10.206.39.50:2181,10.206.39.44:2181,10.206.39.45:2181", 3000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getServers();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 2. 监听服务器的节点信息
    public void getServers() throws KeeperException, InterruptedException {
        // 监听父节点
        List<String> children = zkCli.getChildren(parentNode, true);

        // 创建集合存储服务器列表
        ArrayList<String> serverList = new ArrayList<>();

        for(String c: children) {
            byte[] data = zkCli.getData(parentNode + "/" + c, true, null);
            serverList.add(new String(data));
        }

        // 打印服务器列表
        System.out.println(serverList);
    }

    // 3. 业务逻辑（一直监听）
    public void getWatch() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }
}
