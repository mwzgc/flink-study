package com.mwz.flink.source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mwz.flink.config.DbConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * @author mwz
 */
@Slf4j
public class MySqlSourceExample {

    @SneakyThrows
    public static void main(String[] args) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(DbConfig.hostname)
                .port(3306)
                .databaseList("hoho_test") // set captured database
                .tableList("hoho_test.test_1") // set captured table
                .username(DbConfig.username)
                .password(DbConfig.password)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .serverId("1-4")
                .serverTimeZone("Asia/Shanghai")
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
//        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint");

        SingleOutputStreamOperator<Tuple4<String, Integer, String, Long>> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1)
                .map(data -> {
                    JSONObject jsonObject = JSON.parseObject(data);
                    String op = jsonObject.getString("op");
                    if ("c".equals(op)) {
                        JSONObject after = jsonObject.getJSONObject("after");
                        return new Tuple4<>(op, after.getInteger("id"), after.getString("msg"), after.getLong("create_time"));
                    } else {
                        JSONObject obj = jsonObject.getJSONObject("after");
                        if (Objects.isNull(obj)) {
                            obj = jsonObject.getJSONObject("before");
                        }
                        return new Tuple4<>(op, obj.getInteger("id"), obj.getString("msg"), obj.getLong("create_time"));
                    }
//                    return null;
                }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING, Types.LONG))
                .filter(Objects::nonNull);

        mySQLSource.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }

}
