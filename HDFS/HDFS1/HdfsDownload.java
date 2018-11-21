package com.hdfs.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsDownload {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        //1. 加载配置文件
        Configuration conf = new Configuration();

        //2. 设置副本数
        conf.set("dfs.replication", "2");

        //3. 设置块大小
        conf.set("dfs.blocksize", "64m");

        //4. 构造客户端
        FileSystem fs = FileSystem.get(new URI("hdfs://10.206.39.36:9000/"), conf, "root");

        //5. 从HDFS中下载文件到本地
        fs.copyToLocalFile(new Path("/a.txt"), new Path("/Users/simon/Downloads/aa.txt"));

        fs.close();
    }
}
