package com.holden.com.holden.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @ClassName stock_flink-MyCustomerDeserialization
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月03日11:41 - 周五
 * @Describe
 */
public class MyCustomerDeserialization implements DebeziumDeserializationSchema<String> {
    /*
     * 时间格式:
     * "database":"",
     * "tableName":"",
     * "type":"c u d",
     * "before":{"id":"","tm_name":""...}
     * "after":{"":"","":""...}
     * "ts":12412412
     * */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //Step-1 创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();


        //Step-2 获取主题信息，包含数据库和表名
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];


        //Step-5 获取after数据
        Struct value2 = (Struct) sourceRecord.value();
        Struct after = value2.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            for (Field field : after.schema().fields()) {
                afterJson.put(field.name(), after.get(field.name()));
            }
        }


        //Step-6 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type) || "read".equals(type)) {
            type = "insert";
        }


        //Step-7 将字段写入JSON对象
        result.put("database", db);
        result.put("table", tableName);
        result.put("type", type);
        result.put("after", afterJson);


        //Step- 输出
        collector.collect(result.toString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
