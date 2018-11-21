package com.matrix.simon;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixBean implements Writable {

    private String flag;
    private int index;
    private int num;

    public MatrixBean(String flag, int index, int num) {
        this.flag = flag;
        this.index = index;
        this.num = num;
    }

    public MatrixBean() {}

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return this.flag + "\t" + this.index + "\t" + this.num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.flag);
        out.writeInt(this.index);
        out.writeInt(this.num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.flag = in.readUTF();
        this.index = in.readInt();
        this.num = in.readInt();
    }
}
