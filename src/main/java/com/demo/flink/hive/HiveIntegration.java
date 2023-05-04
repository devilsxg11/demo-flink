package com.demo.flink.hive;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class HiveIntegration {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1. 创建并注册 Hive Catalog
        Catalog catalog = new HiveCatalog("myhive", "tmp", "/app/hive/conf");
        tableEnv.registerCatalog("myhive",  catalog);
        //2. 将 HiveCatalog 设置为当前会话的 Catalog
        tableEnv.useCatalog("myhive");
        //3. 测试调用 Catalog API ，输出 Database
        List<String> databases = catalog.listDatabases();
        System.out.println("++++++++++++++++++++++++++");
        System.out.println(databases);
        System.out.println("++++++++++++++++++++++++++");
        //4. 读取已注册的表并返回结果
        Table test = tableEnv.from("myhive.tmp.sxg_test");
        //5. 使用 Table API 实现一个简单得查询
        Table resultTable = test.select($("*")).limit(10);
        //6. 转为 DataStream 数据流
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        resultStream.print();
        resultStream.writeAsText("hdfs:///tmp/sxg/myhive");

        env.execute();
    }
}
