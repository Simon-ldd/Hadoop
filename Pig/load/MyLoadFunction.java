package com.pig.load;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

public class MyLoadFunction extends LoadFunc {

    private RecordReader reader;

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple result = null;
        try {
            if(!this.reader.nextKeyValue())
                // 表示读完了
                return null;

            result = TupleFactory.getInstance().newTuple();
            Text value = (Text)this.reader.getCurrentValue();
            String str = value.toString();

            // 生成一个Bag
            DataBag bag = BagFactory.getInstance().newDefaultBag();

            // 分词
            String[] words = str.split(" ");
            for (String word: words) {
                Tuple t = TupleFactory.getInstance().newTuple();
                t.append(word);

                bag.add(t);
            }
            result.append(bag);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
