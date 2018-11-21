package com.Order.simon;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {
    //构造函数必须加
    public OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    //重写比较函数
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        if(aBean.getOrderId() > bBean.getOrderId()){
            return 1;
        } else if(aBean.getOrderId() < bBean.getOrderId()){
            return -1;
        } else {
            //这一步会让每个分区只剩一个
            return 0;
        }
    }
}
