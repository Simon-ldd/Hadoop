package com.hdfs.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsIo {
    FileSystem fs = null;
    Configuration conf = null;

    /**
     * 初始化
     */
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();

        fs = FileSystem.get(new URI("hdfs://10.206.39.36:9000/"), conf, "root");
    }

    /**
     * 文件上传
     */
    @Test
    public void putFileToHDFS() throws IOException {
        //1. 获取输入流
        FileInputStream fis = new FileInputStream(new File("/Users/simon/downloads/aa.txt"));

        //2. 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/c.txt"));

        //3. 流的拷贝
        IOUtils.copyBytes(fis, fos, conf);

        //4.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 文件下载
     */
    @Test
    public void getFileFromHDFS() throws IOException {
        //1. 获取输入流
        FSDataInputStream fis = fs.open(new Path("/c.txt"));

        //2. 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("/Users/simon/downloads/a.txt"));

        //3. 流的拷贝
        IOUtils.copyBytes(fis, fos, conf);

        //4. 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }
}
