package com.wordcount.hdfs.simon;

public class WordMapper implements Mapper{

    @Override
    public void map(String line, Context context){
        //1. 拿到这行数据，切分
        String[] words = line.split(" ");

        //2. 拿到单词相同的key, value ++
        for(String word: words){
            context.write(word, 1);
        }
    }
}
