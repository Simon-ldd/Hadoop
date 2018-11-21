package com.simon.hdfs.rpc;

public class MyDatanode implements ClientNamenodeProtocol{

    @Override
    public String getMetaData(String path) {
        return path + ":1 - {BLK_1, BLK_2, BLK_3,...}";
    }
}
