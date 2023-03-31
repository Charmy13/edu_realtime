package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.Dept;
import com.atguigu.gmall.realtime.bean.Emp;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hbase.thirdparty.io.netty.handler.codec.dns.DefaultDnsPtrRecord;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseDataJsonConverter;

import java.time.Duration;

public class FlinkSQLJoin {
    public static void main(String[] args) {

         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                 env.setParallelism(1);
        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(env);

        //间隔时间20秒
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(20));


        //创建socket流读取指定端口数据 类型转换
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 8888)
                .map(
                        lineStr -> {
                            String[] filedArr = lineStr.split(",");
                            return new Emp(Integer.parseInt(filedArr[0]), filedArr[1], Integer.parseInt(filedArr[2]), Long.parseLong(filedArr[3]));
                        }
                );
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 8889).map(
                lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
                }
        );

        //TODO 3.将流转换为动态表
            tableEnv.createTemporaryView("emp",empDS);
            tableEnv.createTemporaryView("dept",deptDS);

        // //TODO 4.内连接测试  --  底层维护两个状态  默认永不过期
        tableEnv.executeSql("select \n" +
                "e.empno,\n" +
                "e.ename,\n" +
                "d.deptno,\n" +
                "d.dname\n" +
                "from emp e\n" +
                "left join  dept d\n" +
                "on e.deptno=d.deptno ").print();


//TODO 5.左外连接测试  左：readAndWrite       右：createAndWrite
        tableEnv.executeSql("select \n" +
                "e.empno,\n" +
                "e.ename,\n" +
                "d.deptno,\n" +
                "d.dname\n" +
                "from emp e\n" +
                "left join dept d");
//todo 6 右外连接
        tableEnv.executeSql("select\n" +
                "e.empno,\n" +
                "e.ename,\n" +
                "d.dept,\n" +
                "d.dname\n" +
                "from emp e\n" +
                "right join dept d\n" +
                "on e.deptno=d.deptno;");

        //todo 7 全外连接
        tableEnv.executeSql("select\n" +
                "e.empno,e.ename,d.deptno,d.dname \n" +
                "\n" +
                "from emp \n" +
                "full join dept d\n" +
                "on e.deptno = d.deptno");

        //todo 8 左连接 数据写到kafka
        tableEnv.executeSql("select \n" +
                "e.empno,\n" +
                "e.deptno\n" +
                "d.empno,\n" +
                "d.deptno\n" +
                "from \n" +
                "emp e \n" +
                "full join\n" +
                "dept d \n" +
                "on e.deptno =d.deptno\n");

        //TODO 8.左连接数据写到kafka
        /*left join 实现过程
        假设A表作为主表与B表做等值左外联。
        当A表数据进入算子，而B表数据未至时会先生成一条B表字段均为null的关联数据ab1，其标记为 +I。
        其后，B表数据到来，会先将之前的数据撤回，即生成一条与ab1内容相同，但标记为-D的数据，再生成一条关联后的数据，标记为 +I。
        这样生成的动态表对应的流称之为回撤流。
        这里如果将left join 之后的数据写到kafka主题的话，会有null值,我们不能使用kafka connector，需要使用kafka upsert connector；
        如果使用new FlinkKafkaConsumer<String>("topic",new SimpleStringSchema(),prop);消费数据， SimpleStringSchema进行
        序列化的时候，要求value不能为空，会报错；所以我们自己重写反序列化方法。*/

        tableEnv.executeSql("create table emp\n" +
                "empno bigint ,\n" +
                "ename String ,\n" +
                "deptno bigIny,\n" +
                "proc_time as proctime()\n" +
                "with connector ='kafka'\n" +
                "topic='first'\n" +
                "properties.bootstrap-servers =hadoop102:9092\n" +
                "properties.group.id='testGroup',\n" +
                "scan.startup.mode='latest-offset',\n" +
                "value.format='json'\n");





        try {
                     env.execute();
                 } catch (Exception e) {
                     throw new RuntimeException(e);
                 }



    }
}
