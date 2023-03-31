package com.atguigu.gmall.realtime.common;

import com.atguigu.gmall.realtime.bean.Dept;
import com.atguigu.gmall.realtime.bean.Emp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoin {
    public static void main(String[] args) {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                 env.setParallelism(1);


        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 8888).map(
                lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    return new Emp(Integer.parseInt(fieldArr[0]), fieldArr[1], Integer.parseInt(fieldArr[2]), Long.parseLong(fieldArr[3]));
                }
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Emp>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Emp>() {
                                    @Override
                                    public long extractTimestamp(Emp emp, long recordTimestamp) {
                                        return emp.getTs();
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 8889).map(
                lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
                }
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Dept>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Dept>() {
                                    @Override
                                    public long extractTimestamp(Dept dept, long recordTimestamp) {
                                        return dept.getTs();
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<Tuple2<Emp, Dept>> joinedDS = empDS.keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.seconds(-5),Time.seconds(5))
                .process(
                        new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                            @Override
                            public void processElement(Emp emp, Dept dept, Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                                out.collect(Tuple2.of(emp, dept));
                            }
                        }
                );

        joinedDS.print(">>>>");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

//一条流和另外一条流指定时间 范围进行关联


