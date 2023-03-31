package com.atguigu.gmall.realtime.util;

public class MySqlUtil {

    //获取从MySQL中读取字典表数据创建动态表的建表语句
    public static String getBaseDicDDL() {
        return "CREATE TABLE base_dic (\n" +
                "  dic_code string,\n" +
                "  dic_name string,\n" +
                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + getJdbcDDL("base_dic");
    }

    //获取jdbc连接器连接属性
    public static String getJdbcDDL(String tableName) {
        return "WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall?useSSL=false',\n" +
                "   'table-name' = '" + tableName + "',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '000000',\n" +
                "   'lookup.cache' = 'PARTIAL',\n" +
                "   'lookup.partial-cache.max-rows' = '200',\n" +
                "   'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                "   'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }
}
