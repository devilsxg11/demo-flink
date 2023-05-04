package com.demo.flink.sql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/3/5.
 * Description:
 */
public class TemporalTableFunctionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 获取 stream 和 table 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 提供一个汇率历史记录表静态数据集
        List<Tuple2<String, Long>> ratesHistoryData = new
                ArrayList<>();
        ratesHistoryData.add(Tuple2.of("USD", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        // 3. 创建临时视图，普通表
        DataStream<Tuple2<String, Long>> ratesHistoryStream =
                env.fromCollection(ratesHistoryData);

//        Table ratesHistory = tableEnv.fromDataStream(ratesHistoryStream,Schema.newBuilder()
//                        .column("r_currency", "STRING")
//                        .column("r_rate", "Long")
//                        .watermark("r_proctime", "PROCTIME()")
//                        .build());
        Table ratesHistory = tableEnv.fromDataStream(ratesHistoryStream,
                $("r_currency"), $("r_rate"), $("r_proctime").proctime());
        tableEnv.createTemporaryView("rates_history", ratesHistory);

        // 4. 创建和注册时态表函数
        // 指定 "r_proctime" 为时间属性，指定 "r_currency" 为主键
        TemporalTableFunction rates = tableEnv
                .from("rates_history")
                .createTemporalTableFunction( $("r_proctime"),
                        $("r_currency"));

        tableEnv.createTemporarySystemFunction("rates", rates);

        // 5. 提供一个订单表数据集
//        List<Tuple2<String, Integer>> ordersData = new
//                ArrayList<>();
//        ordersData.add(Tuple2.of("Euro", 2));
//        ordersData.add(Tuple2.of("USD", 1));
//        ordersData.add(Tuple2.of("Yen", 50));
//        ordersData.add(Tuple2.of("Euro", 3));
//        ordersData.add(Tuple2.of("USD", 5));
//        DataStream<Tuple2<String, Integer>> orderStream =
//                env.fromCollection(ordersData);

        // 6. 创建订单表
        DataStream<String> source = env.socketTextStream("10.115.88.47",9999);
        DataStream<Tuple2<String, Integer>> orderStream = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Integer.valueOf(arr[1]));
            }
        });

        Table orders = tableEnv.fromDataStream(orderStream,
                $("currency"), $("amount"), $("rowtime").proctime());

        tableEnv.createTemporaryView("orders", orders);

        // 7. 关联
        Table resultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "o.amount * r.r_rate AS amount,o.rowtime  " +
                        "FROM " +
                        "orders AS o, " +
                        "LATERAL TABLE (rates(o.rowtime)) AS r " +
                        "WHERE r.r_currency = o.currency");

        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream.print();
        env.execute();

    }
}
