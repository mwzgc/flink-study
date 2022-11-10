package com.mwz.flink.table;

import com.mwz.flink.config.DbConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;


/**
 * @author mwz
 */
public class TableExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        settings.getConfiguration().setString("execution.checkpointing.interval", "60s");

        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    id  INT,\n" +
                "    amount      INT,\n" +
                "    create_time TIMESTAMP(3),\n" +
                "    PRIMARY KEY (id) NOT ENFORCED," +
                "    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
//                "    'server-id' =  '5400-5408',\n" +
//                "    'scan.startup.mode' =  'latest-offset',\n" +
                "    'server-time-zone' =  'Asia/Shanghai',\n" +
                "    'hostname'     = '" + DbConfig.hostname + "',\n" +
                "    'port' = '3306',\n" +
                "    'username'    = '" + DbConfig.username + "',\n" +
                "    'password'    = '" + DbConfig.password + "',\n" +
                "    'database-name'    = 'hoho_test',\n" +
                "    'table-name'    = 'test_2'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE spend_report  (\n" +
                "    id  INT,\n" +
                "    amount      INT,\n" +
                "    create_time TIMESTAMP(3),\n" +
                "    PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector'  = 'jdbc',\n" +
                "    'url'        = 'jdbc:mysql://" + DbConfig.hostname + ":3306/hoho_test',\n" +
                "    'table-name' = 'test_3',\n" +
                "    'driver'     = 'com.mysql.cj.jdbc.Driver',\n" +
                "    'username'   = '" + DbConfig.username + "',\n" +
                "    'password'   = '" + DbConfig.password + "'\n" +
                ")");

//        Table transactions = tEnv.from("transactions");
//        transactions
//                .window(Tumble.over(lit(10).second()).on($("create_time")).as("log_ts"))
//                .groupBy($("log_ts"))
//                .select($("log_ts").start().as("log_ts"), $("amount").sum().as("amount"))
//                .execute().print();


        tEnv.executeSql("SELECT DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm') create_time, sum(amount) amount\n" +
                "FROM transactions\n" +
                "GROUP BY DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm')").print();

//                tEnv.executeSql("SELECT * from transactions").print();
    }

    public static Table report(Table transactions) {
        return transactions;
    }

}
