package com.mapcache.simon;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class CacheReducer extends Reducer<Text, CacheBean, CacheBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<CacheBean> values, Context context) throws IOException, InterruptedException {
        TreeSet<CacheBean> treeSet = new TreeSet<>();
        for (CacheBean value: values) {
            treeSet.add(value);
            if(treeSet.size() > 10)
                treeSet.pollLast();
        }
        for (CacheBean value: treeSet) {
            context.write(value, NullWritable.get());
        }
    }
}
