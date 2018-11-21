package com.wordcount.mapreduce.simon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        compress("/Users/simon/downloads/simon/inCompress/Archive.zip", "org.apache.hadoop.io.compress.DefaultCodec");
//        compress("/Users/simon/downloads/simon/inCompress/Archive.zip", "org.apache.hadoop.io.compress.BZip2Codec");
        compress("/Users/simon/downloads/simon/inCompress/Archive.zip", "org.apache.hadoop.io.compress.GzipCodec");
    }

    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        // 1. 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));

        Class cName = Class.forName(method);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(cName, new Configuration());

        // 2. 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));

        // 3. 创建压缩输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);

        // 4. 流的对拷
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 2, false);

        // 5. 关闭资源
        fis.close();
        cos.close();
        fos.close();
    }
}
