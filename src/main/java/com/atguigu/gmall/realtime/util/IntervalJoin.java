package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.Dept;
import com.atguigu.gmall.realtime.bean.Emp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.TimestampsAndWatermarksOperator;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

/**
 *
 */
public class IntervalJoin {

    public static void main(String[] args) {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                 env.setParallelism(1);
              env.socketTextStream("hadoop102",8888)
                      .map(
                              lineStr->{
                                  String[] fieldArr = lineStr.split(",");
                                  return new Emp (Integer.parseInt(fieldArr[0]), fieldArr[1], Integer.parseInt(fieldArr[2]), Long.parseLong(fieldArr[3]));
                              }
                      ).assignTimestampsAndWatermarks(WatermarkStrategy.<Emp>forMonotonousTimestamps()
                              .withTimestampAssigner(
                                      new SerializableTimestampAssigner<Emp>() {
                                          @Override
                                          public long extractTimestamp(Emp emp, long recordTimestamp) {
                                              return emp.getTs();
                                          }
                                      }
                              ));

/*
   env.socketTextStream("hadoop102",8889)
           .map(lineStr-> {
               String[] fileArr = lineStr.split(",");
               return new Dept (Integer.parseInt(fileArr[0]),fileArr[1],Long.parseLong(fileArr[2]));
           }).assignTimestampsAndWatermarks(
                   WatermarkStrategy.forMonotonousTimestamps()
           )
*/





        try {
                     env.execute();
                 } catch (Exception e) {
                     throw new RuntimeException(e);
                 }

    }
}
