package com.simon.hdfs.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 创建客户端
 */

public class Client {

    public static void main(String[] agrs) throws IOException {
        //1. 拿到协议
        ClientNamenodeProtocol proxy = RPC.getProxy(ClientNamenodeProtocol.class, 1L, new InetSocketAddress("localhost", 7777),
                new Configuration());

        //2. 发送请求
        String metaData = proxy.getMetaData("/simon.txt");

        //3. 拿到元数据信息
        System.out.println(metaData);
    }
}
