package com.wordcount.hdfs.simon;

/**
 * 实现Mapper接口
 * 思路：
 *  添加一个map方法，单词切分，相同key的value++
 */
public class WordCountMapper implements Mapper{

    @Override
    public void map(String line, Context context){
        //1. 拿到这行数据，切分
        String[] words = line.split(" ");

        //2. 拿到单词相同的key, value ++
        for(String word: words){
            Object value = context.get(word);
            if(null == value)
                context.write(word, 1);
            else{
                int v = (int)value;
                context.write(word, v + 1);
            }
        }
    }
}
