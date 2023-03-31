package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.TableProcess;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.guava30.com.google.common.collect.Table;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 * 维表
 * 数据来源： kafka
 * 做了什么？ 获取到的字符串准换成对
 *
 * 历史首日全量同步的数据
 *
 *
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置
    /*    env.enableCheckpointing(3000L,CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //job取消之后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
       //设置两个检查点之间的最小间隔时间
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)));
        env.setStateBackend(new HashMapStateBackend());
        //设置状态后端
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/edu/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/


        //TODO 3.从kafka的主题读取数据

        String topic ="edu_topic_db";
        String groupId="dim_app_group";

        KafkaSource<String> source = KafkaUtil.getKafkaSource(topic, groupId);
        //消费数据封装为流
        DataStreamSource<String> kafkaDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        //TODO 4.对读取的数据进行类型的转换以及简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
                        //将jsonStr
                        JSONObject jsonObject=JSON.parseObject(jsonStr);
                        String type = jsonObject.getString("type");
                        if(!"bootstrap-start".equals(type)&&!"bootstrap-complete".equals(type)){
                            out.collect(jsonObject);
                        }
                    }
                }
        );
        //TODO 5.使用FlinkCDC读取配置表中的数据---配置流
        Properties props = new Properties();
        props.setProperty("useSSL","false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("edu_config")
                .tableList("edu_config.table_process")
                .username("root")
                .password("000000")
                .jdbcProperties(props)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql Source");

        //将读取到的配置流进行广播 广播流
        MapStateDescriptor<String,TableProcess> mapStateDescriptor=new MapStateDescriptor<String, TableProcess>(
                "mapStateDescriptor",
                String.class,
                TableProcess.class
        );
        //将读取到的配置表广播出去


        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);

        //将主流和广播流进行关联
        BroadcastConnectedStream<JSONObject, String> connectDS= jsonObjDs.connect(broadcastDS);
        //对关联之后的数据进行处理(过滤出维度)
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        dimDS.print(">>>>");
        dimDS.addSink(new DimSinkFunction());
        env.execute();
    }
}
