package com.flowcount.mapreduce.partition.simon;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 由于数据中需要统计上行、下行、流量总和，这里我们自定义一个数据类
 */

public class FlowBean implements Writable {
    //定义属性
    private long upFlow;
    private long dfFlow;
    private long flowSum;

    //空参构造函数
    public FlowBean(){}

    //有参构造函数
    public FlowBean(long upFlow, long dfFlow){
        this.upFlow= upFlow;
        this.dfFlow = dfFlow;
        this.flowSum = upFlow + dfFlow;
    }

    //定义外部接口
    public void setUpFlow(long upFlow){
        this.upFlow = upFlow;
    }
    public long getUpFlow(){
        return this.upFlow;
    }

    public void setDfFlow(long dfFlow){
        this.dfFlow = dfFlow;
    }
    public long getDfFlow(){
        return this.dfFlow;
    }

    public void setFlowSum(long flowSum){
        this.flowSum = flowSum;
    }
    public long getFlowSum(){
        return this.flowSum;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.dfFlow);
        out.writeLong(this.flowSum);
    }

    //反序列化，注意：序列化和反序列化的顺序要相同
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.dfFlow = in.readLong();
        this.flowSum = in.readLong();
    }

    @Override
    public String toString() {
        return this.upFlow + "\t" + this.dfFlow + "\t" + this.flowSum;
    }
}
