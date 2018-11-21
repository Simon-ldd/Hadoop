package com.wordcount.hdfs.simon;

/**
 * 思路:
 *  接口设计
 */
public interface Mapper {

    //通用的方法，把输入的数据打散，并存入到数据传输的类中
    public void map(String line, Context context);
}
