package com.wordcount.hdfs.simon;

import java.util.HashMap;

/**
 * 思路：
 *  数据传输的类
 *  封装数据
 *  集合
 *  采用Object具有通用性
 */
public class Context {

    //数据封装
    private HashMap<Object, Object> contextMap = new HashMap<>();

    //写数据
    public void write(Object key, Object value) {
        this.contextMap.put(key, value);
    }

    //根据key获取value的方法
    public Object get(Object key) {
        return this.contextMap.get(key);
    }

    //拿到map当中的数据内容
    public HashMap<Object, Object> getContextMap() {
        return this.contextMap;
    }
}
