package com.bytedance.simon;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UtimeBean implements Writable {
    private String uid;
    private String time;

    public UtimeBean(String uid, String time) {
        this.uid = uid;
        this.time = time;
    }

    public String getUid(){
        return this.uid;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeUTF(time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.time = in.readUTF();
    }
}
