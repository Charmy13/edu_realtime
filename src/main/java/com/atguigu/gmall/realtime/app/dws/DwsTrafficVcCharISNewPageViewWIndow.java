package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsTrafficVcCharISNewPageViewWIndow {
    // TODO: 2023/3/18
    public static void main(String[] args) {
        //todo 1 基本环境准备
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                 env.setParallelism(4);

        //todo 2 检查点设置
//        env.getCheckpointConfig()

//        todo 3从kafka的页面日志中读取数据
      //消费主题 消费者组
        //消费者对象
       //去哪儿照这个topic
        String topic ="dwd_traffic_page_log";
        String topic_group="dws_traffic_channel_page_view_window";

        //创建消费者对象  封装过
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, topic_group);
        //消费数据转换成流
        //贴近数据源添加水位线的方法？
        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        //对读取数据类型转换


        //lamda 将jsonstr 转换成 jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaDs.map(jsonstr -> JSON.parseObject(jsonstr));


        //todo 5 按照mid 分组
        //将 
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //todo 6 统计独立访客数 会话数 页面浏览数据 页面访问时长 封装实体类


        try {
                     env.execute();
                 } catch (Exception e) {
                     throw new RuntimeException(e);
                 }

    }
    //todo 8 按照维度进行分组（底层）

    //todo 9 开窗


//生产环境中的问题？（没懂）

    //触发计算的时间
    //数据乱序
    //数据迟到

    //窗口关闭时间

    //迟到时间调大

    //todo 10 聚合计算

    //reduce 累加窗口中的每一个元素
    //aggregateFunction  传入参数累加器
    //如果输入输出以及累加器的类型相同 选后者

    //todo 11 将聚合的记过写到clickhouse
//数据流写入

}
