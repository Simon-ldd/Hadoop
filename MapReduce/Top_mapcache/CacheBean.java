package com.mapcache.simon;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CacheBean implements WritableComparable<CacheBean> {

    private String uid;
    private String url;
    private String time;

    public CacheBean(String uid, String url, String time) {
        this.uid = uid;
        this.url = url;
        this.time = time;
    }
    public CacheBean(){}

    public void setUid(String uid) {
        this.uid = uid;
    }
    public String getUid(){
        return this.uid;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    public String getUrl() {
        return this.url;
    }

    public void setTime(String time) {
        this.time = time;
    }
    public String getTime(){
        return this.time;
    }

    @Override
    public String toString() {
        return this.uid + "\t" + this.url + "\t" + this.time;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uid);
        out.writeUTF(this.url);
        out.writeUTF(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.url = in.readUTF();
        this.time = in.readUTF();
    }

    @Override
    public int compareTo(CacheBean o) {
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        try {
            Date t1 = ft.parse(this.time);
            Date t2 = ft.parse(o.getTime());
            if(t1.after(t2)) {
                return -1;
            } else {
                return 1;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return 0;
    }
}
