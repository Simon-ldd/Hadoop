package com.bytedance.simon;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OrderBean implements WritableComparable<OrderBean> {

    private String url;
    private String time;

    public OrderBean(String url, String time) {
        this.url = url;
        this.time = time;
    }

    public String getTime(){
        return this.time;
    }

    public String toString() {
        return this.url + "\t" + this.time;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(url);
        out.writeUTF(time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.url = in.readUTF();
        this.time = in.readUTF();
    }

    @Override
    public int compareTo(OrderBean o) {
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            Date t1 = ft.parse(this.time);
            Date t2 = ft.parse(o.getTime());
            if(t1.before(t2))
                return -1;
            else
                return 1;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 1;
    }
}
