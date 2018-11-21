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

public class HdfsReadWrite {
    FileSystem fs = null;

    /**
     * 初始化
     */
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1. 加载配置
        Configuration conf = new Configuration();

        //2. 设置副本
        conf.set("dfs.replication", "2");

        //3. 设置块大小
        conf.set("dfs.blocksize", "64m");

        //4. 创建客户端
        fs = FileSystem.get(new URI("hdfs://10.206.39.36:9000/"), conf, "root");
    }

    /**
     * 读数据方式1
     */
    @Test
    public void hdfsRead1() throws IOException {
        //1. 打开hdfs中的文件
        FSDataInputStream in = fs.open(new Path("/aaa.txt"));

        //2. 创建字节数组
        byte[] buf = new byte[1024];

        //3. 读取到字节数组
        in.read(buf);

        //4. 输出结果
        System.out.println(new String(buf));

        //5. 关闭文件流
        in.close();
        fs.close();
    }

    /**
     * 读数据方式2
     */
    @Test
    public void hdfsRead2() throws IOException {
        //1. 打开hdfs中的文件
        FSDataInputStream in = fs.open(new Path("/aaa.txt"));

        //2. 创建缓冲流提高读取效率
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));

        //3. 按行读取
        String line = null;
        while ((line = br.readLine()) != null){
            System.out.println(line);
        }

        //4. 关闭缓冲流
        br.close();
        in.close();
        fs.close();
    }

    /**
     * 读取数据方式3
     */
    @Test
    public void hdfsRead3() throws IOException {
        //1. 打开hdfs中的文件
        FSDataInputStream in = fs.open(new Path("/aaa.txt"));

        //2. 指定偏移量
        in.seek(6);

        //3. 创建字节数组
        byte[] b = new byte[8];

        //4. 读取数据到字节数组
        in.read(b);

        //5. 输出结果
        System.out.println(new String(b));

        in.close();
        fs.close();
    }

    /**
     * 写数据
     */
    @Test
    public void hdfsWrite() throws IOException {
        //1. 创建hdfs中的文件
        FSDataOutputStream out = fs.create(new Path("/b.txt"), true);

        //2. 写数据
        out.write("simon henshuai henshuai".getBytes());

        //3. 关闭流
        IOUtils.closeStream(out);
        fs.close();
    }
}
