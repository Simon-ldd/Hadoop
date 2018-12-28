package com.simon.hbasemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HbaseDriver implements Tool {

    private Configuration conf;

    public int run(String[] strings) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 创建任务
        Job job = Job.getInstance(conf);

        // 2. 指定运行的主类
        job.setJarByClass(HbaseDriver.class);

        // 3. 配置job采用scan方式扫描该表
        Scan scan = new Scan();

        // 4. 设置Mapper类
        TableMapReduceUtil.initTableMapperJob("users", scan, ReadHbaseMapper.class, ImmutableBytesWritable.class, Put.class, job);

        // 5. 设置Reducer类
        TableMapReduceUtil.initTableReducerJob("usermr", WriteHbaseReducer.class, job);

        // 设置reduceTask个数
        job.setNumReduceTasks(1);

        boolean rs = job.waitForCompletion(true);
        return rs? 0:1;
    }

    // 设置配置
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    // 拿到配置
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new HbaseDriver(), args);
        System.exit(status);
    }
}
