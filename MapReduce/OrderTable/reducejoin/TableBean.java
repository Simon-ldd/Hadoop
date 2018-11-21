package com.simon.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    // 封装对应的字段
    private String orderId;
    private String pId;
    private int pMount;
    private String pName;
    private String flag;

    public TableBean() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getpMount() {
        return pMount;
    }

    public void setpMount(int pMount) {
        this.pMount = pMount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.pId);
        out.writeInt(this.pMount);
        out.writeUTF(this.pName);
        out.writeUTF(this.flag);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.pId = in.readUTF();
        this.pMount = in.readInt();
        this.pName = in.readUTF();
        this.flag = in.readUTF();
    }

    public String toString() {
        return this.orderId + "\t" + this.pName + "\t" + this.pMount;
    }
}
