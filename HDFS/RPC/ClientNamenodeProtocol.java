package com.simon.hdfs.rpc;

/**
 * 协议是大家都需要遵循的，这里采用接口的方式
 * 根据实际需要实现不同的协议内容
 */
public interface ClientNamenodeProtocol {

    //1. 定义协议的ID
    public static final long versionID = 1L;

    //2. 定义方法（拿到元数据的方式）, 因为上传下载一定会指定路径，这里将路径作为传入参数
    public String getMetaData(String path);
}
