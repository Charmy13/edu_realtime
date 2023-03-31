package com.atguigu.gmall.realtime.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试demo
 */
public class FLinkSQl {
    public static void main(String[] args) {
     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
             env.setParallelism(1);

             //表执行环境
        StreamTableEnvironment tableEnv =StreamTableEnvironment.create(env);
        //创建动态表
        tableEnv.executeSql("CREATE TABLE user_info (\n" +
                "id INT,\n" +
                "name STRING,\n" +
                "age INT,\n" +
                "primary key(id) not enforced\n" +
                " WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '000000'," +
                "'database-name' = 'gmall_config'," +
                "'table-name' = 't_user'" +
                ")");



             try {
                 env.execute();
             } catch (Exception e) {
                 throw new RuntimeException(e);
             }


    }


}
