package com.holden.com.holden.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName stock_flink-StockTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月03日18:57 - 周五
 * @Describe
 */
public class StockTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 配置连接
        DebeziumSourceFunction<String> mysql = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("spider_base")
                .tableList("spider_base.stock_test")
                .serverTimeZone("UTC")
                .deserializer(new MyCustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();

        //Step-3 连接source数据源
        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        mysqlDS.keyBy(x -> 1).map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public String map(String value) throws Exception {
                return null;
            }
        }).print();


        env.execute();
    }
}
