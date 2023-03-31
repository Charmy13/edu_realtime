package com.atguigu.gmall.realtime.common;

/**
 * 定义常量类
 */
public class GmallConfig {

    public static final String PHOENIX_SCHEMA = "EDU_REALTIME";
    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix连接参数
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
