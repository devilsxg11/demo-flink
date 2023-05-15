package com.demo.flink.type;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/2/7.
 * Description:
 */
public class TypeDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WordWithCount> wordCounts = env.fromElements(
                new WordWithCount("hello", 1),
                new WordWithCount("world", 2));

        wordCounts.map(new MapFunction<WordWithCount, WordWithCount>() {
            @Override
            public WordWithCount map(WordWithCount value) throws Exception {
                return value;
            }
        }).keyBy(value -> value.word).print();

        env.execute(TypeDemo.class.getSimpleName());
    }

}



