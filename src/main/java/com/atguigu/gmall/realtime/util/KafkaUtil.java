package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

public class KafkaUtil {
    static final String  KAFKA_SERVER ="hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(topic)
                .setGroupId(groupId)
                //从flink状态中维护的偏移量位置开发消费，如果状态中还没有维护偏移量，从kafka的最新位置开发消费
                //如果要做如下配置：需要将检查点打开,生产环境这么设置
                // .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //为了保证一致性，在消费数据的时候，我们这里只读取已提交的消息
                // .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                //kafka从不同偏移量 开始消费
                // 在学习的时候，我们可以直接从kafka最新位置开始读取
                .setStartingOffsets(OffsetsInitializer.latest())

                //注意：如果使用SimpleStringSchema进行反序列化的话，不能处理空消息
                // .setValueOnlyDeserializer(new SimpleStringSchema())  一共有两种schema 构造器一种是key  一种是value
                //将消息反序列化 不能处理空消息
                //将输入数据转换为 Kafka 的 ProducerRecord schema构造器
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            //将消息反序列化 不能处理空消息
                            @Override
                            public String deserialize(byte[] message) throws IOException {

                               if(message !=null){
                                   return new String(message);
                               }
                               return null;
                            }

                            @Override
                            public boolean isEndOfStream(String string) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                               return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        return kafkaSource;
    }

   public static KafkaSink<String> getKafkaSink(String topic)

   {
       //默认broker端： transaction.max.timeout.ms=15min
       //客户端的超时时间：不允许超过这个值，而它的默认： transaction.timeout.ms = 1h
       //两个解决方法：
       //①在客户端：pros.put(“transaction.timeout.ms”, 15 * 60 * 1000);（小于15min）
       // 两阶段提交 保证一致性  大于等于检查点的时间 小于等于默认15分钟


       //②在服务端：修改配置文件：transaction.max.timeout.ms设置超过1小时

/*       Properties properties =new Properties();
       properties.put("transaction.timeout.ms",15*60*1000);*/


       KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
               .setBootstrapServers(KAFKA_SERVER)
               //序列化
               .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                       .setTopic(topic)
                       .setValueSerializationSchema(new SimpleStringSchema())
                       .build()
               )
               //生产的精确一次性

               //生产的精确一次性消费 每一个流数据在做向kafka 写入的事故后都会生成一个transId 默认名字生成规则相同 指定前缀 区分
//               .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//指定事务id 前缀
//               .setTransactionalIdPrefix(transId)
               .build();
       return kafkaSink;
   }


    //获取从topic_db主题中读取数据创建动态表的建表语句
    public static String getTopicDbDDL(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `ts` string,\n" +
                "  `data` MAP<string, string>,\n" +
                "  `old` MAP<string, string>,\n" +
                "  proc_time as proctime()\n" +
                ") " + getKafkaDDL("topic_db", groupId);
    }

    //获取kafka连接器相关的连接属性
    public static String getKafkaDDL(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                //在生产环境下，为了保证消费的一致性，需要做如下的配置
                // "  'scan.startup.mode' = 'group-offsets',\n" +
                // "  'properties.auto.offset.reset' = 'latest'\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    //获取upsert-kafka连接器相关的连接属性
    public static String getUpsertKafkaDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
    public static FlinkKafkaConsumer<String> getKafkaConsumer (String topic ,String groupID) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", KAFKA_SERVER);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String string) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        return null;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return null;
                    }
                }
                , prop);
        return consumer;
    }

    public static <T>KafkaSink<T> getKafkaSinkBySchema(KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema)
    {
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .build();
        return kafkaSink;
    }
}


