package com.demo.flink.watermark;

import com.demo.flink.kafka.entity.AnimalEvent;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/1/14.
 * Description:
 */
public class WatermarkDemo {

    public static void main(String[] args) {
        // 1.创建 Stream 执行上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 设置 checkpoint
        CheckpointConfig ck = env.getCheckpointConfig();
        //2.1 每隔 5 秒执行一次
        env.enableCheckpointing(5000);
        //2.2 设置 checkpoint 模式，精准一次
        ck.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置 checkpoint 超时时间
        ck.setCheckpointTimeout(60 * 1000);
        // 2.4 退出不删除 checkpoint，当作业取消时，保留作业的 checkpoint
        ck.setExternalizedCheckpointCleanup(CheckpointConfig.
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5 状态后端配置，这里设置外部文件系统（1.16 中已废弃）
        StateBackend stateBackend = new FsStateBackend("file:///flink/checkpoint/wm-demo");
        env.setStateBackend(stateBackend);

        // 设置全局并行度
        env.setParallelism(1);

        // 3. 监听系统的 9999 端口，创建一个socket 数据源
        DataStream<String> source = env.socketTextStream("10.115.88.47",9999);

        // 4. 使用 map 函数处理数据里转化为 AnimalEvent 对象
        SingleOutputStreamOperator<AnimalEvent> ds = source.flatMap(new FlatMapFunction<String, AnimalEvent>() {
            @Override
            public void flatMap(String s, Collector<AnimalEvent> collector) throws Exception {
                String[] arr = s.split(" ");
                if (arr.length >= 4) {
                    collector.collect(new AnimalEvent(Integer.valueOf(arr[0]),
                            arr[1], Long.valueOf(arr[2]), arr[3]));
                }
            }
        });

        // 5. 定义 “旁路输出”收集迟到的数据
        final OutputTag<AnimalEvent> EVENT_LAG_TAG = new
                OutputTag<>("EVENT_LAG_TAG", TypeInformation.of(AnimalEvent.class));

        // 6. 设置水印，窗口，延时时间
        SingleOutputStreamOperator<AnimalEvent> commonDs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.
                <AnimalEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getClientTime()))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(0))
                .sideOutputLateData(EVENT_LAG_TAG)
                .apply(new AllWindowFunction<AnimalEvent, AnimalEvent, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<AnimalEvent> values,
                                      Collector<AnimalEvent> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
                        System.out.println("当前时间窗口 start: " + sdf.format(window.getStart()) +
                                "，end: " + sdf.format(window.getEnd()));
                        for (AnimalEvent event : values) {
                            out.collect(event);
                        }
                    }
                });

        // 7. 打印正常事件
        commonDs.addSink(new SinkFunction<AnimalEvent>() {
            @Override
            public void invoke(AnimalEvent value, Context context) throws Exception {
                System.out.println(" IN Order event: " +  value);
            }

            @Override
            public void writeWatermark(Watermark watermark) throws Exception {
                System.out.println("Watermark: "  +  watermark.getTimestamp() + "， " +  watermark.getFormattedTimestamp());
            }
        });
        // 8. 打印迟到事件
        DataStream<AnimalEvent> lagDs = commonDs.getSideOutput(EVENT_LAG_TAG);
        lagDs.addSink(new SinkFunction<AnimalEvent>() {
            @Override
            public void invoke(AnimalEvent value, Context context) throws Exception {
                System.out.println("Lag event: " +  value);
            }
        });

        //9. 执行程序
        try {
            env.execute("Watermark Demo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
