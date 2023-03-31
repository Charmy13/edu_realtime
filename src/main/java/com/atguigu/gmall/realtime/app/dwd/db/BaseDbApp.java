package com.atguigu.gmall.realtime.app.dwd.db;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BaseDbApp {
    public static void main(String[] args) {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                 env.setParallelism(4);

                 //todo 2检查点相关设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());



                 try {
                     env.execute();
                 } catch (Exception e) {
                     throw new RuntimeException(e);
                 }

    }
}
