package com.simon.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;

public class CacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    // 将商品信息加载到内存中
    private HashMap<String, String> pMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 加载缓存文件
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pd.txt"), "utf-8"));

        String line;
        while(StringUtils.isNotEmpty(line = br.readLine())){
            this.pMap.put(line.toString().split("\t")[0], line.toString().split("\t")[1]);
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String line = value.toString();
        //切割
        String[] fields = line.split("\t");
        //获取商品id
        String pName = this.pMap.get(fields[1]);
        //输出
        context.write(new Text(fields[0] + "\t" + pName + "\t" + fields[2]), NullWritable.get());
    }
}
