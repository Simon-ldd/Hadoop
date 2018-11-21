package com.simon.hdfs.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

/**
 * 模拟一个hdfs采用rpc通信的过程
 * 主要是客户端向namenode节点请求元数据的模拟
 *
 * 采用rpc通信，就需要遵从一定的通信协议
 */
public class PublishServer {

    public static void main(String[] args) throws IOException {
        //1. 构建RPC框架
        Builder builder = new RPC.Builder(new Configuration());

        //2. 绑定地址
        builder.setBindAddress("localhost");

        //3. 绑定端口号
        builder.setPort(7777);

        //4. 绑定协议，这里我们自己定义协议
        builder.setProtocol(ClientNamenodeProtocol.class);

        //5. 调用协议的实现类
        builder.setInstance(new MyNamenode());

        //6. 创建服务
        Server server = builder.build();
        server.start();
    }
}
