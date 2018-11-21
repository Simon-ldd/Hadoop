package com.mapcache.simon;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class CacheMapper extends Mapper<LongWritable, Text, Text, CacheBean> {

    HashMap<String, String> buMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("ub.txt"), "utf-8"));

        String line;
        while(StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split("\t");
            buMap.put(fields[1], fields[0]);
        }

        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String day = fields[2].substring(0, 10);
        String uid = this.buMap.get(fields[0]);

        context.write(new Text(uid + day), new CacheBean(uid, fields[1], fields[2]));
    }
}
