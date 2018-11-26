package com.ZooKeeper.zkCli;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkClient {

    private String connectString = "10.206.39.50:2181,10.206.39.37:2181,10.206.39.45:2181";
    private int sessionTimeout = 3000;
    private ZooKeeper zkCli = null;

    // 初始化客户端
    @Before
    public void init() throws IOException {
        zkCli = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            // 回调监听
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Path: " + event.getPath() + "\tState: " + event.getState() + "\tType: " + event.getType());

                try {
                    List<String> children = zkCli.getChildren("/", true);

                    for (String c: children) {
                        System.out.println(c);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 创建子节点
    @Test
    public void createZnode() throws KeeperException, InterruptedException {

        // ZooDefs.Ids.OPEN_ACL_UNSAFE, 设置应答方式：开放式应答
        // CreateMode.PERSISTENT, 设置创建类型
        String path = zkCli.create("/simon_test", "simonhenshuaia".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    // 获取子节点
    @Test
    public void getChild() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/", true);
        for (String c: children)
            System.out.println(c);
    }

    // 删除节点
    @Test
    public void rmChild() throws KeeperException, InterruptedException {
        byte[] data = zkCli.getData("/simon_test", true, null);
        System.out.println("/simon_test中的数据: " + new String(data));

        // -1表示删除所有的版本
        zkCli.delete("/simon_test", -1);
    }

    // 修改数据
    @Test
    public void setData() throws KeeperException, InterruptedException {
        zkCli.setData("/simon_test", "simon18".getBytes(), -1);
        System.out.println("/simon_test修改后的数据: " + new String(zkCli.getData("/simon_test", true, null)));
    }

    // 判断节点是否存在
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/simon_test", false);
        System.out.println(exists == null? "not exists" : "exists");
    }

    @After
    public void cleanup() throws InterruptedException {
        zkCli.close();
    }
}
