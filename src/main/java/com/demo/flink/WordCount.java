package com.demo.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright：gome.com.cn
 * Author: SongXiaoGuang
 * Date: 2022/12/19.
 * Description:
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建 Stream 执行上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 监听系统的9999端口，创建一个socket数据源
        DataStream<String> ds = env.socketTextStream("10.115.88.47",9999);
        // 3. 使用 map 函数将获取到的每一行数据以空格分割，每个单词计数
        ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for(String word: arr){
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        })
        // 4. 以单词分组、求和、打印
        .keyBy(t->t.f0)
        .sum(1)
        .print();
        // 5. 执行程序
        env.execute();
    }
}
