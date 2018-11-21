package com.bytedance.simon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class OrderReducer extends Reducer<UtimeBean, OrderBean, Text, OrderBean> {

    @Override
    public void reduce(UtimeBean key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        TreeSet<OrderBean> tree = new TreeSet<>();

        for(OrderBean value: values) {
            tree.add(value);
            if(tree.size() > 10)
                tree.pollLast();
        }

        for (OrderBean value: tree){
            context.write(new Text(key.getUid()), value);
        }
    }
}
