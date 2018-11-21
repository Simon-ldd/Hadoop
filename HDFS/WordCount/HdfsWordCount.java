package com.wordcount.hdfs.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * 需求：统计文件中每个单词出现的次数
 *  数据存储在hdfs中，统计结果也存到hdfs中
 *
 * 2004 Google：dfs/bigtable/mapreduce
 *
 * 思路：
 *  hello 1
 *  simon 1
 *  ...
 *
 * 基于用户体验：
 *  用户输入数据
 *  用户指定处理方式
 *  用户指定结果的存储路径
 * 采用配置文件的方式
 *
 * MapReduce思想：
 *  例子：一个汉堡的做成
 *  1. map阶段：数据打散，将各种材料切片
 *  2. reduce阶段：合成，将各种切片材料合成一个汉堡
 *
 * 配置文件：用户指定输入、输出、业务逻辑
 * Mapper接口：map工作，map的统称，根据不同的业务逻辑实现不同的类
 * 实现Mapper接口，WordCountMapper，单词统计的业务逻辑实现
 * Context类，一个通用的数据传输的类
 */

public class HdfsWordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, URISyntaxException, InterruptedException {
        //通过配置文件反射
        Properties pro = new Properties();

        //加载配置文件
        pro.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties"));

        //拿到路径
        Path inpath = new Path(pro.getProperty("IN_PATH"));
        Path outpath = new Path(pro.getProperty("OUT_PATH"));
        //Class.forName动态加载类
        Class<?> mapper_class = Class.forName(pro.getProperty("MAPPER_CLASS"));

        //实例化，使用newInstance()
        Mapper mapper = (Mapper)mapper_class.newInstance();

        Context context = new Context();

        //构造HDFS客户端对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://10.206.39.36:9000/"), conf, "root");

        //读取用户输入文件
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(inpath, false);
        while(iter.hasNext()) {
            LocatedFileStatus file = iter.next();
            //打开文件，获取输入流
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));

            String line = null;
            while((line = br.readLine()) != null) {
                //调用map方法
                mapper.map(line, context);
            }

            //关闭资源
            br.close();
            in.close();
        }

        //将缓存的结果存入hdfs中
        HashMap<Object, Object> contextMap = context.getContextMap();
        FSDataOutputStream out = fs.create(outpath);

        //遍历hashmap
        Set<Entry<Object, Object>> entrySet = contextMap.entrySet();

        for(Entry<Object, Object> entry: entrySet) {
            //写数据
            out.write((entry.getKey().toString() + "\t" + entry.getValue().toString() + "\n").getBytes());
        }

        out.close();
        fs.close();

        System.out.println("数据统计结果完成------");
    }
}
