package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.util.Collection;
import java.util.Set;
//将 维度流中的数据写到phoenix 表中
public class DimSinkFunction   implements SinkFunction<JSONObject> {

    //将流中的数据保存到phoenix中的不同的维度表中

    @Override
    public void invoke(JSONObject value,Context context) throws Exception {
    //获取维度的输出的目的表名

        String tableName =value.getString("sink_table");
        //将JSONObject中的所有属性保存到phoenix 表中 需要将输出目的地从jsonObj中删除掉
        value.remove("sink_table");
        String upsertSql="upsert into "+GmallConfig.PHOENIX_SCHEMA+"."+tableName+
                " ( " + StringUtils.join(value.keySet(), ",") + ") " +
                " values " +
                " ('" + StringUtils.join(value.values(), "','") + "') ";
        System.out.println("向phoenix表中插入数据的语句为"+upsertSql);

        //调用向phoenix表中插入数据的方法
        PhoenixUtil.executeSql(upsertSql);


    }


}
