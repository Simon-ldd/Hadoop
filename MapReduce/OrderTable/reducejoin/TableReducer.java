package com.simon.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 创建集合，存放订单数据
        ArrayList<TableBean> orderbean = new ArrayList<>();

        // 存储对应id的商品信息
        TableBean pd = new TableBean();

        for(TableBean v : values) {
            if("0".equals(v.getFlag())) {
                TableBean tmp = new TableBean();
                try {
                    BeanUtils.copyProperties(tmp, v);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderbean.add(tmp);
            } else {
                try {
                    BeanUtils.copyProperties(pd, v);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // 拼接
        for(TableBean tableBean: orderbean) {
            tableBean.setpName(pd.getpName());
            context.write(tableBean, NullWritable.get());
        }
    }
}
