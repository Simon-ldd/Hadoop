package com.flowcount.mapreduce.sort.simon;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    //定义属性
    private long upFlow;
    private long dfFlow;
    private long flowSum;

    //无参构造
    public FlowBean(){}

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDfFlow() {
        return dfFlow;
    }

    public void setDfFlow(long dfFlow) {
        this.dfFlow = dfFlow;
    }

    public long getFlowSum() {
        return flowSum;
    }

    public void setFlowSum(long flowSum) {
        this.flowSum = flowSum;
    }

    @Override
    public String toString() {
        return this.upFlow + "\t" + this.dfFlow + "\t" + this.flowSum;
    }

    //有参构造
    public FlowBean(long upFlow, long dfFlow){

        this.upFlow = upFlow;
        this.dfFlow = dfFlow;
        this.flowSum = upFlow + dfFlow;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.dfFlow);
        out.writeLong(this.flowSum);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.dfFlow = in.readLong();
        this.flowSum = in.readLong();
    }

    //排序
    @Override
    public int compareTo(FlowBean o) {
        //倒序
        return this.flowSum > o.getFlowSum() ? -1 : 1;
    }
}
