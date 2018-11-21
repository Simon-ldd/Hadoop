package com.hdfs.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFS1 {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        //1. 客户端访问，必然先加载配置文件
        Configuration conf = new Configuration();

        //2. 指定配置（设置成2个副本）
        conf.set("dfs.replication", "2");

        //3. 指定块的大小
        conf.set("dfs.blocksize", "64m");

        //4. 构造客户端，相当于创建一个文件流
        FileSystem fs = FileSystem.get(new URI("hdfs://10.206.39.36:9000/"), conf, "root");

        //5. 上传文件
        fs.copyFromLocalFile(new Path("/Users/simon/downloads/a.txt"), new Path("/aaaa.txt"));

        //6. 关闭客户端/关闭文件流
        fs.close();
    }
}
