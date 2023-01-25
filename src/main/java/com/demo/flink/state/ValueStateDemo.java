package com.demo.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/1/23.
 * Description:
 */
public class ValueStateDemo {

    public static void main(String[] args) throws Exception {
        // 1.创建 Stream 执行上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 监听系统 9999端口，创建一个socket数据源，示例：apple 1, banana 2，orange 3 ...
        DataStream<String> ds = env.socketTextStream("10.115.88.47",9999);
        // 3. keyBy 操作，调用自定义 map 函数，实现累加
        ds.keyBy((KeySelector<String, String>) value -> {
            String[] arr = value.split(" ");
            return arr[0];
        }).map(new SumMapFun()).print();

        // 5. 执行程序
        env.execute();
    }

    // 4. 自定义 RichMapFunction 实现 sum 操作
    static class SumMapFun extends RichMapFunction<String, Tuple2<String,Long>> {

        // 声明 ValueSate
        private ValueState<Long> sumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 定义 state 描述符
            ValueStateDescriptor<Long> sumDescriptor = new ValueStateDescriptor<>(
                    "sum-state", LongSerializer.INSTANCE );
            // 注册 state
            sumState = getRuntimeContext().getState(sumDescriptor);
        }

        @Override
        public Tuple2<String, Long> map(String value) throws Exception {
            // 读取 state
            Long accValue = sumState.value();
            if(accValue == null){
                accValue = 0L;
            }
            // 实现累加
            String[] arr = value.split(" ");
            String key = arr[0];
            Long currentValue = Long.valueOf(arr[1]);
            Long currentSumValue = accValue + currentValue;
            // 更新 state
            sumState.update(currentSumValue);
            return new Tuple2<>(key, currentSumValue);
        }
    }
}
