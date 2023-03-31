package com.atguigu.gmall.realtime.bean;

import lombok.Data;

@Data
public class TableProces {
    // 来源表
    String source_table;
    String source_type;
    String  sink_table;
    String sink_type;
    String  sink_columns;
    String  sink_pk;
    String   sink_extend;

}
