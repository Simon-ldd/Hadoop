package com.Order.simon;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    //定义属性
    private int orderId;
    private double price;

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public OrderBean(){}

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return this.orderId + "\t" + this.price;
    }

    @Override
    public int compareTo(OrderBean o) {
        if(this.orderId > o.getOrderId()){
            return 1;
        } else if(this.orderId < o.getOrderId()){
            return -1;
        } else {
            return this.price > o.getPrice()? -1 : 1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.orderId);
        out.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.price = in.readDouble();
    }
}
