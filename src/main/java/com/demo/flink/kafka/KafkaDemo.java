package com.demo.flink.kafka;

import com.alibaba.fastjson.JSONObject;
import com.demo.flink.kafka.entity.KafkaEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

/**
 * Copyright：gome.com.cn
 * Author: SongXiaoGuang
 * Date: 2023/1/2.
 * Description:
 */
public class KafkaDemo {
    public static void main(String[] args) {
        // 1. 获取一个执行环境 execution environment，
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. ParameterTool 提供了读取程序启动参数、配置文件、
        // 环境变量以及自身配置参数等配置的的一个工具类
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        // 3. 设置重启策略，控制在发生故障时如何重新启动作业。这里以固定延迟重启策略为例：
        env.getConfig().setRestartStrategy(RestartStrategies.
                fixedDelayRestart(4, 10000));

        // 4. 设置 checkpoint
        CheckpointConfig ck = env.getCheckpointConfig();
        //4.1 每隔 5 秒执行一次
        env.enableCheckpointing(5000);
        //4.2 设置 checkpoint 模式，精准一次
        ck.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //4.3 设置 checkpoint 超时时间
        ck.setCheckpointTimeout(60 * 1000);
        // 4.4 退出不删除 checkpoint，当作业取消时，保留作业的 checkpoint
        ck.setExternalizedCheckpointCleanup(CheckpointConfig.
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 4.5 状态后端配置，这里设置外部文件系统（1.16 中已废弃）
        StateBackend stateBackend = new FsStateBackend("file:///flink/checkpoint/kafka-demo");
        env.setStateBackend(stateBackend);
        // 5. 设置全局并行度
        env.setParallelism(2);
        // 6. 定义 kafka 数据源
        KafkaSource<KafkaEvent> kafkaSource = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("event_topic")
                .setGroupId("kafka-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new KafkaEventSchema())
                .build();

        // 6. 读取数据
        DataStream<KafkaEvent> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // 7. 转换操作
        DataStream<Tuple2<String ,Long>> ds = source.filter(new FilterFunction<KafkaEvent>() {
            @Override
            public boolean filter(KafkaEvent kafkaEvent) throws Exception {
                return kafkaEvent != null;
            }
        }) // 过滤
                .map(new MapFunction<KafkaEvent, KafkaEvent>() {
                    @Override
                    public KafkaEvent map(KafkaEvent kafkaEvent) throws Exception {
                        System.out.println("map: " + kafkaEvent);
                        return kafkaEvent;
                    }
                })// 打印原始数据
                .keyBy(KafkaEvent::getEvent) // 按 event 分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) //基于 process time 的滚动窗口
                .apply(new WindowFunction<KafkaEvent, Tuple2<String ,Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<KafkaEvent> input,
                                      Collector<Tuple2<String ,Long>> out) throws Exception {
                        Long count = 0L;
                        for (KafkaEvent record : input) {
                            count++;
                        }
                        long start = window.getStart();
                        long end = window.getEnd();
                        System.out.println("start time: " + start + ", end time: " +
                                end + ", event: " + s + ", 累计次数：" + count);
                        out.collect(new Tuple2<>(s, count));
                    }
                }).shuffle();

        // 8. 打印输出
        ds.print();
        try {
            env.execute("Kafka Demo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class KafkaEventSchema implements DeserializationSchema<KafkaEvent> {

        @Override
        public KafkaEvent deserialize(byte[] bytes) throws IOException {
            String msg = new String(bytes, StandardCharsets.UTF_8);
            if(StringUtils.isNotBlank(msg)){
                return JSONObject.parseObject(msg, KafkaEvent.class);
            }
            return null;
        }

        @Override
        public boolean isEndOfStream(KafkaEvent kafkaEvent) {
            return false;
        }

        @Override
        public TypeInformation<KafkaEvent> getProducedType() {
            return TypeInformation.of(KafkaEvent.class);
        }
    }
}
