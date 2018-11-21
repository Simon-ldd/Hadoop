package com.simon.hdfs.rpc;

/**
 * 实现协议接口的类
 * 模拟返回元数据的信息
 */

public class MyNamenode implements ClientNamenodeProtocol{

    @Override
    public String getMetaData(String path) {
        return path + ":3 - {BLK_1, BLK_2, BLK_3,...}";
    }
}
