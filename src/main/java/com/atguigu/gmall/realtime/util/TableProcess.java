package com.atguigu.gmall.realtime.util;

import lombok.Data;

@Data
public class TableProcess {
    //来源表
    String sourceTable;
    // 来源操作类型
    String source_type;
    //'输出表'
    String sink_table;
    //'输出类型: dim | dwd'
    String sink_type;
    // '输出字段',
    String sink_columns;
    //'主键字段'
    String sink_pk;
//    建表扩展
    String sink_extend;


    public String getSource_type() {
        return source_type;
    }

    public String getSink_pk() {
        return sink_pk;
    }

    public String getSink_extend() {
        return sink_extend;
    }

    public String getSink_columns() {
        return sink_columns;
    }

    public String getSink_table() {
        return sink_table;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSink_type() {
        return sink_type;
    }

    public void setSink_type(String sink_type) {
        this.sink_type = sink_type;
    }
}
