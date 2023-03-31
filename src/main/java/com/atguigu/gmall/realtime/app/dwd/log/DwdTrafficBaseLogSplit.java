package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

public class DwdTrafficBaseLogSplit {

    public static void main(String[] args) {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                 env.setParallelism(4);
      /*  //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/gmall/ck");
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");   */

        //todo 3 从kafka
        String topic="topic_log";
        String groupId="dwd_traffic_base_log_split_group";

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS= env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka Source");

        //侧输出流 收集 dirtyTag

        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        SingleOutputStreamOperator<JSONObject> processDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    //如果在转换的过程中没有发生异常，说明是标准的json字符串，将jsonStr转换为jsonObj，传递到下游
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                } catch (Exception e) {
                    //如果在转换的过程中发生了异常，说明不是标准的json字符串，属于脏数据，放到侧输出流中
                    ctx.output(dirtyTag, jsonStr);
                }
            }
        });

        //TODO 5.将侧输出流中脏数据写到kafka主题中
        SideOutputDataStream<String> dirtyDS = processDS.getSideOutput(dirtyTag);
        KafkaSink<String>  kafkaSink= KafkaUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);


        try {
                     env.execute();
                 } catch (Exception e) {
                     throw new RuntimeException(e);
                 }

    }
}
