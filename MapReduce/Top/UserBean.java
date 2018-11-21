package com.bytedance.simon;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserBean implements Writable {
    private String sid;
    private String uid;
    private String url;
    private String time;

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String toString(){
        return this.uid + "\t" + this.url + "\t" + this.time;
    }

    public UserBean(String sid, String uid, String url, String time) {
        this.sid = sid;
        this.uid = uid;
        this.url = url;
        this.time = time;
    }

    public UserBean(){
        this.sid = "";
        this.uid = "";
        this.url = "";
        this.time = "";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.sid);
        out.writeUTF(this.uid);
        out.writeUTF(this.url);
        out.writeUTF(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sid = in.readUTF();
        this.uid = in.readUTF();
        this.url = in.readUTF();
        this.time = in.readUTF();
    }
}
