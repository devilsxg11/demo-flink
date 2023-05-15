package com.demo.flink.type;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/2/7.
 * Description:
 */
public class WordWithCount {

    public String word;
    public int count;

    public WordWithCount() {}

    public WordWithCount(String word, int count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
