package com.demo.flink.state.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/1/24.
 * Description:
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {

        // 1.创建 Stream 执行上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 监听系统 9999 端口，模拟用户事件流，格式化将字符串转化 Action 为对象
        // 样例样例数据：1001 login, 1002 addCart，1003 logout
        DataStream<String> actions = env.socketTextStream("10.115.88.47",9999);
        SingleOutputStreamOperator<Action> formatAction = actions.map(new MapFunction<String, Action>() {
            @Override
            public Action map(String value) throws Exception {
                String[] arr = value.split(" ");
                return new Action(Long.valueOf(arr[0]), arr[1]);
            }
        });
        // 3. 监听系统 8888 端口，模拟规则流，格式化将字符串转化 Pattern 为对象
        // 样例样例数据：login logout，pay logout
        DataStream<String> patterns = env.socketTextStream("10.115.88.47",8888);
        SingleOutputStreamOperator<Pattern> formatPattern = patterns.map(new MapFunction<String, Pattern>() {
            @Override
            public Pattern map(String value) throws Exception {
                String[] arr = value.split(" ");
                return new Pattern(arr[0], arr[1]);
            }
        });
        // 定义一个 MapState 描述符，k/v 格式
        MapStateDescriptor<Void, Pattern> bcStateDescriptor =
                new MapStateDescriptor<>("patterns", Types.VOID, TypeInformation.of(Pattern.class));
        // 利用 pattern 数据流生成 BroadcastStream
        BroadcastStream<Pattern> bcPatterns = formatPattern.broadcast(bcStateDescriptor);
        // 4. 按用户分组，连接 规则流，在自定义 KeyedBroadcastProcessFunction 中实现用户行为和规则匹配的逻辑
        formatAction.keyBy(Action::getUid)
                .connect(bcPatterns)
                .process(new PatternEvaluator())
                .print("match pattern：");

        // 5. 执行程序
        env.execute();
    }

    static class PatternEvaluator extends KeyedBroadcastProcessFunction<Long, Action, Pattern, Tuple2<Long, Pattern>>{

        // 使用 keyed state 存储前一个事件的用户行为
        ValueState<String> preActionSate;
        // broadcast state 描述符，map类型的结构，由于不需要 key 指，这里指定 Void 类型
        MapStateDescriptor<Void, Pattern> patternDesc;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化 state
            preActionSate = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "action-state", String.class));
            patternDesc = new MapStateDescriptor<>("patterns",
                    Types.VOID, TypeInformation.of(Pattern.class));
        }

        // 用户行为流
        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<Long, Pattern>> out) throws Exception {
            System.out.println("current user action: " +  value.toString());
            // 从当前的 broadcast state 获取当前定义的规则
            Pattern pattern = ctx.getBroadcastState(patternDesc).get(null);

            // 从当前用户的 ValueState 获取上一个 action
            String prevAction = preActionSate.value();
            if(prevAction != null && pattern != null){
                // check 是否匹配规则
                if(pattern.getPrevAction().equals(prevAction) &&
                        pattern.getNextAction().equals(value.getAction())){
                    // 匹配后，输出当前用户和规则
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 每次更新当前用户上一个 action 到 keyed state
            preActionSate.update(value.action);
        }

        // 规则流，处理每一条规则事件
        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<Long, Pattern>> out) throws Exception {
            System.out.println("current set pattern: " +  value.toString());
            //更新最新的事件流到 BroadcastState
            BroadcastState<Void, Pattern> bcPatternState = ctx.getBroadcastState(patternDesc);
            bcPatternState.put(null, value);
        }
    }
}
