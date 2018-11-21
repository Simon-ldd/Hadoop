package com.bytedance.simon;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ClickReducer extends Reducer<Text, UserBean, UserBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<UserBean> values, Context context) throws IOException, InterruptedException {
        UserBean tmp = new UserBean();
        ArrayList<UserBean> arry = new ArrayList<>();

        for (UserBean value: values) {
            if(value.getUrl().equals("")){
                tmp.setSid(value.getSid());
                tmp.setUid(value.getUid());
            } else {
                if(!tmp.getSid().equals("")){
                    value.setUid(tmp.getUid());
                    context.write(value, NullWritable.get());
                } else {
                    arry.add(value);
                }
            }
        }

        for(UserBean value: arry) {
            value.setUid(tmp.getUid());
            context.write(value, NullWritable.get());
        }
    }
}
