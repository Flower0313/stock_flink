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
            private MapState<Integer, Double> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Double>("map", Integer.class, Double.class));
            }

            @Override
            public void close() throws Exception {
                mapState.clear();
            }

            @Override
            public String map(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value).getJSONObject("after");
                int number = Integer.parseInt(jsonObj.get("number").toString());
                double score = 0.0;
                double diff = Double.parseDouble(jsonObj.get("diff").toString());
                if (!"1".equals(String.valueOf(number))) {
                    Double lastScore = mapState.get(number - 1);
                    score = (diff + 4 * lastScore) / 5;
                    mapState.put(number, score);
                } else {
                    score = Double.parseDouble(jsonObj.get("score").toString());
                    mapState.put(number, Double.parseDouble(jsonObj.get("score").toString()));
                }


                String result = number + "\t\t" + diff + "\t\t" + score;
                return result;
            }
        }).print();


        env.execute();
    }
}
