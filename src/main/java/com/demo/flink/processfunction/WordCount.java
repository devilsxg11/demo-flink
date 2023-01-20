package com.demo.flink.processfunction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/01/20.
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
        // 5.process function
        .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer> >() {
            // 5.1 定义 state 存储中间结果
            private ValueState<Integer> accState;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 定义 state 描述符
                ValueStateDescriptor<Integer> wordDescriptor = new ValueStateDescriptor<>(
                        "sum-state",
                        Integer.class
                );
                accState = getRuntimeContext().getState(wordDescriptor);
                super.open(parameters);
            }

            // 5.2  timer 的定义，这里使用默认实现
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx,
                                Collector<Tuple2<String, Integer> > out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }

            // 5.2 每个元素处理
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx,
                                       Collector<Tuple2<String, Integer> > out) throws Exception {
                // 5.2.1 输出元数据信息
                TimerService timerService = ctx.timerService();
                //System.out.println("currentProcessingTime：" +
                        //timerService.currentProcessingTime() + "，currentWatermark：" + timerService.currentWatermark());
                //System.out.println("Key：" + ctx.getCurrentKey() + "，input element：" + value);

                // 5.2.2 输出元数据信息
                Integer accValue = accState.value();
                if(accValue == null){
                    accState.update(value.f1);
                }else {
                    accState.update(accValue + value.f1);
                }
                // 5.2.3 输出 word 统计结果
                out.collect(new Tuple2<>(value.f0, accState.value()));
            }
        })
        .print();
        // 5. 执行程序
        env.execute();
    }
}
