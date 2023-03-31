package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.MySqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.检查点相关的设置(略)

        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表
        //声明消费的主题以及消费者组
        String groupId = "dwd_trade_cart_add_group";
        tableEnv.executeSql(KafkaUtil.getTopicDbDDL(groupId));

        // tableEnv.executeSql("select * from topic_db").print();
        //TODO 4.过滤出加购行为
        Table cartAdd = tableEnv.sqlQuery("select\n" +
                "   `data`['id'] id,\n" +
                "   `data`['user_id'] user_id,\n" +
                "   `data`['sku_id'] sku_id,\n" +
                "   if(`type`='insert',`data`['sku_num'],cast((cast(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) as string)) sku_num,\n" +
                "   `data`['source_type'] source_type,\n" +
                "   ts,\n" +
                "   proc_time\n" +
                "from topic_db\n" +
                "where `table`='cart_info' \n" +
                "and (`type`='insert' or (`type`='update' and `old`['sku_num'] is not null " +
                "and CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT) ))");
        tableEnv.createTemporaryView("cart_add",cartAdd);

        //TODO 5.从MySQL数据库中读取字典表数据 创建动态表
        tableEnv.executeSql(MySqlUtil.getBaseDicDDL());
        // tableEnv.executeSql("select * from base_dic").print();
        //TODO 6.将加购表和字典表进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "    cadd.id,\n" +
                "    cadd.user_id,\n" +
                "    cadd.sku_id,\n" +
                "    cadd.sku_num,\n" +
                "    cadd.ts,\n" +
                "    cadd.source_type,\n" +
                "    dic.dic_name source_type_name\n" +
                "FROM cart_add AS cadd JOIN base_dic FOR SYSTEM_TIME AS OF cadd.proc_time AS dic\n" +
                "    ON cadd.source_type = dic.dic_code");
        tableEnv.createTemporaryView("joined_table",joinedTable);
        // tableEnv.executeSql("select * from joined_table").print();

        //TODO 7.将关联的结果写到kafka主题
        //7.1 创建动态表和要写入的kafka主题进行映射
        tableEnv.executeSql("CREATE TABLE dwd_trade_cart_add (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  sku_num string,\n" +
                "  ts string,\n" +
                "  source_type string,\n" +
                "  source_type_name string,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + KafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add"));
        //7.2 写入
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from joined_table");

    }
}
