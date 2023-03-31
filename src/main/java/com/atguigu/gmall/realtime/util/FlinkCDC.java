package com.atguigu.gmall.realtime.util;




import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.tracing.SpanReceiverInfo;

import java.util.Properties;


public class FlinkCDC {
    public static void main(String[] args) {


          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                  env.setParallelism(1);


//                  检查点
        // TODO 2. 开启检查点   Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,
        // 需要从Checkpoint或者Savepoint启动程序
        // 2.1 开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置超时时间为 1 分钟
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 2.3 设置两次重启的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        // 2.4 设置任务关闭的时候保留最后一次 CK 数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1L), Time.minutes(1L)
        ));
        // 2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/flinkCDC"
        );
        // 2.7 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");




                  try {
                      env.execute();
                  } catch (Exception e) {
                      throw new RuntimeException(e);
                  }

    }
}
