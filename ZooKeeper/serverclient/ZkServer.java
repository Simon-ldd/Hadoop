package com.ZooKeeper.serverclient;

import org.apache.zookeeper.*;

import java.io.IOException;

// 服务端
public class ZkServer {
    private ZooKeeper zkCli = null;
    private String parentNode = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 1. 创建连接
        ZkServer zkServer = new ZkServer();
        zkServer.getConnect();

        // 2. 注册信息
        zkServer.regist(args[0]);

        // 3. 业务逻辑处理
        zkServer.build(args[0]);
    }

    // 1. 连接zkServer
    public void getConnect() throws IOException {
        zkCli = new ZooKeeper("10.206.39.50:2181,10.206.39.44:2181,10.206.39.45:2181", 3000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    // 2. 注册信息， 即是创建子节点
    public void regist(String hostname) throws KeeperException, InterruptedException {
        String node = zkCli.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(node);
    }

    // 3. 构造服务器, 模拟一个服务器
    public void build(String hostname) throws InterruptedException {
        System.out.println(hostname + ": 服务器上线了!");
        Thread.sleep(Long.MAX_VALUE);
    }
}
