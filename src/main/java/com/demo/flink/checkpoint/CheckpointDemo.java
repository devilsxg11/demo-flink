package com.demo.flink.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/1/28.
 * Description:
 */
public class CheckpointDemo {

    public static void main(String[] args) throws Exception {
        // 1. 获取一个执行环境
        Configuration conf = new Configuration();
        // 重启时指定checkpoint目录
        conf.setString("execution.savepoint.path",
                "file:///D:/flink/checkpoint/ck-demo/f4fd6e15cd31d784f81509fc55ebcfe9/chk-26");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf);
        env.setStateBackend(new HashMapStateBackend());

        // 2. 设置 checkpoint
        // 2.1 启用 checkpoint，5s 做一次快照，并且设置语义“精准一次”
        env.enableCheckpointing(5000 ,CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig ck = env.getCheckpointConfig();
        //2.2 设置 checkpoint 超时时间
        ck.setCheckpointTimeout(60 * 1000);
        // 2.3 退出不删除 checkpoint，当作业取消时，保留作业的 checkpoint
        ck.setExternalizedCheckpointCleanup(CheckpointConfig.
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置 CheckpointStorage
        ck.setCheckpointStorage(new
                FileSystemCheckpointStorage("file:///flink/checkpoint/ck-demo"));

        // 3. 设置全局并行度
        env.setParallelism(2);

        // 4. kafka 数据源 apple 1, apple 2, banana 1, orange 1
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sum-topic")
                .setGroupId("sum-kafka-group-1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 5. 读取 source 数据
        DataStream<String> ds = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // 6 计算逻辑
        ds.keyBy((KeySelector<String, String>) value -> {
            String[] arr = value.split(" ");
            return arr[0];
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, Tuple2<String,Long>
                        , String, TimeWindow>() {

                    private ValueState<Long> sumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Long> sumDescriptor = new ValueStateDescriptor<>(
                                "sum-state", LongSerializer.INSTANCE);
                        sumState = getRuntimeContext().getState(sumDescriptor);
                    }
                    @Override
                    public void process(String key, Context context, Iterable<String> elements,
                                        Collector<Tuple2<String, Long>> out) throws Exception {
                        // 读取 state
                        Long accValue = sumState.value();
                        if (accValue == null) {
                            accValue = 0L;
                        }
                        // 实现累加
                        long currentValue = 0L;
                        for (String element : elements) {
                            String[] arr = element.split(" ");
                            currentValue += Long.parseLong(arr[1]);
                        }
                        Long currentSumValue = accValue + currentValue;
                        // 更新 state
                        sumState.update(currentSumValue);
                        System.out.println("current state value：" + accValue +
                                ",current sum value：" + currentSumValue);
                        out.collect(new Tuple2<>(key, currentSumValue));
                    }
                })
                .print("current sum result：");

        // 7. 执行程序
        env.execute();

    }
}
