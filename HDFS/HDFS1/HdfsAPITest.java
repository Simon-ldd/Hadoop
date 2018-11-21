package com.hdfs.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsAPITest {

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
     * 创建文件夹
     * hdfs dfs -mkdir 文件夹名
     */
    @Test
    public void hdfsMkdir() throws IOException {
        //1. 创建文件夹
        fs.mkdirs(new Path("/simon"));

        //2. 关闭文件流
        fs.close();
    }

    /**
     * 移动/修改文件名
     * hdfs dfs -mv 源文件路径 目标路径
     */
    @Test
    public void hdfsRename() throws IOException {
        //1. 移动文件夹并改名
        fs.rename(new Path("/a.txt"), new Path("/simon/b.txt"));

        //2. 关闭流
        fs.close();
    }

    /**
     * 删除文件
     * hdfs dfs -rm -r 文件路径
     */
    @Test
    public void hdfsRm() throws IOException {
        //1. 删除文件
        fs.delete(new Path("/aa.txt"), true);

        //2. 关闭流
        fs.close();
    }

    /**
     * 获取目录信息
     * hdfs dfs -ls 目录
     */
    @Test
    public void hdfsLs() throws IOException {
        //1. 通过远程迭代器获取目录信息
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);

        //2. 循环打印目录信息
        while(iter.hasNext()) {
            LocatedFileStatus status = iter.next();
            System.out.println("文件路径为: " + status.getPath());
            System.out.println("块大小: " + status.getBlockSize());
            System.out.println("文件大小: " + status.getLen());
            System.out.println("副本数: " + status.getReplication());
            System.out.println("块信息: " + Arrays.toString(status.getBlockLocations()));
            System.out.println("==================");
        }

        //3. 关闭文件流
        fs.close();
    }

    /**
     * 判断是文件还是文件夹
     */
    @Test
    public void hdfsJuge() throws IOException {
        //1. 获取状态信息
        FileStatus[] list = fs.listStatus(new Path("/"));

        //2. 遍历
        for(FileStatus ls: list) {
            if(ls.isFile()){
                System.out.println("文件: " + ls.getPath().getName());
            } else {
                System.out.println("文件夹: " + ls.getPath().getName());
            }
        }

        //3. 关闭文件流
        fs.close();
    }
}
