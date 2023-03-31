package com.atguigu.gmall.realtime.app.dws;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                 env.setParallelism(4);

                 env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
                 //每隔1小时做一次检查点的备份
                 env.getCheckpointConfig().setCheckpointTimeout(60000L);
                 //设置job取消后 检查点是否保留
                env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000l);
                env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.days(30)));
                env.setStateBackend(new HashMapStateBackend());
                //检查点存储的路径

                //指定 操作hadoop 的用户
                System.setProperty("HADOOP_USER_NAME","atguigu");
        try {
                     env.execute();
                 } catch (Exception e) {
                     throw new RuntimeException(e);
                 }

    }
}
