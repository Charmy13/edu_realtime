package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProces;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSutil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import com.atguigu.gmall.realtime.util.TableProcess;
import com.ibm.icu.number.SkeletonSyntaxException;
import com.sun.org.apache.xpath.internal.objects.XNull;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Table;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.MalformedParameterizedTypeException;
import java.sql.*;
import java.util.*;

/**
 * dim层处理主流数据以及广播流数据
 * 无法保证维度信息的全表扫描 在动态分流引用启动后hi行 此外故障重启是，主流和广播流数据的加载顺序同样无法控制
 * 需要对配置表i新你先进行预加载只要在flink
 *
 * */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
   private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private HashMap<String,TableProcess> configMap=new HashMap<>();


    public  TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    //程序一开始执行
    @Override
    public void open(Configuration parameters) throws Exception {
       //如果 主流数据先到 配置流后到的情况
        //主流数据先到 而广播流数据后到的情况
        // 在生命周期开始 在配置表中读取一次配置信息放到mapConfig中
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection conn=DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?"+
                "user=root&password=000000&useUnicode=true&characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        String sql= "select * from edu_config.table_process where sink_type='dim'";
        PreparedStatement ps=conn.prepareStatement(sql);
        //处理结果集
        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while(resultSet.next()){
            JSONObject jsonObject=new JSONObject();
            for(int i=1;i<metaData.getColumnCount();i++){
                String columnName = metaData.getColumnName(i);
                //输出的列名
                Object columnValue = resultSet.getObject(i);
                jsonObject.put(columnName,columnValue);
            }
            TableProcess tableProcess = JSON.toJavaObject(jsonObject, TableProcess.class);
            configMap.put(tableProcess.getSourceTable(),tableProcess);
        }
        resultSet.close();
        ps.close();
        conn.close();
        }




    //处理主流业务数据
    public void processElement (JSONObject jsonObject,ReadOnlyContext ctx ,Collector<JSONObject> out) throws Exception {
        //获取主流中操作的业务数据库的表名
        String tableName = jsonObject.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //根据表名到广播状态中获取对应的配置信息
        TableProcess tableProcess=null;

    //如果配置信息不为空 说明当前主流中处理的是维度数据
        if((tableProcess=broadcastState.get(tableName))!=null || (tableProcess=configMap.get(tableName))!=null) {
            //当前处理的业务表数据维度数据
            JSONObject dataObject = jsonObject.getJSONObject("data");
            //在向下游传递前，过滤掉不需要传递的属性
            String sink_columns = tableProcess.getSink_columns();
            //在向下游传递前 过滤掉不需要的传递的属性
            filterColumn(dataObject, sink_columns);
            //在向下游传递前 需要将输出的目的地补充上
            String sink_table = tableProcess.getSink_table();
            dataObject.put("sink_table", sink_table);
            //在向下游传递之前 补充操作类型
            dataObject.put("type", jsonObject.getString("type"));

            //将data-维度的变化内容 传递到下游
            out.collect(dataObject);
        }
    }


    //处理广播流数据
    public  void processBroadcastElement (String jsonStr,Context ctx,Collector<JSONObject> out) throws Exception {

        //将jsonStr 转换为jsonObj
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        //获取操作类型
        System.out.println("输出广播流的数据"+jsonObject.toJSONString());
        String op = jsonObject.getString("op");
        //删除
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if("d".equals(op)){
            TableProcess object = jsonObject.getObject("before", TableProcess.class);
            String sink_type = object.getSink_type();
            if("dim".equals(sink_type)){
                //根据对象获取表名
                String sourceTable = object.getSourceTable();
                //根据表名将配置信息从广播状态中删除掉
                broadcastState.remove(sourceTable);
                configMap.remove(sourceTable);
            }
        }else{
            //删除外的其他操作
            TableProcess after = jsonObject.getObject("after", TableProcess.class);
            String sink_type = after.getSink_type();
            if("dim".equals(sink_type)){
                //获取业务数据库表名
                String sourceTable = after.getSourceTable();
                String sink_table = after.getSink_table();
                String sink_columns = after.getSink_columns();
                String sink_pk = after.getSink_pk();
                String sink_extend = after.getSink_extend();
                checkTable(sink_table,sink_columns,sink_pk,sink_extend);

                //将读取的配置放到广播状态中
                broadcastState.put(sourceTable,after);
               configMap.put(sourceTable,after);
            }
        }

    }


    //拼接phenix中的维度表 建表语句
    private void checkTable (String tableName ,String sinkColumns,String pk,String ext){
        //空值处理
        if(pk==null){
            pk="id";
        }if(ext==null){
            ext="";
        }
        //拼接建表语句
        StringBuilder createSql =new StringBuilder("create table if not exists "+GmallConfig.PHOENIX_SCHEMA+"."+tableName+" (");
        String[] columnArr= sinkColumns.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String columnName = columnArr[i];
            if(columnName.equals(pk)){
                createSql.append(columnName+" varchar primary key");//建表语句空格  sgg 书写问题
            }
            else  {
                createSql.append(columnName+" varchar ");
            }
            //除了最后一个字段 后面都需要加
            if(i<columnArr.length-1){
                createSql.append(" , ");
            }
        }
        createSql.append(" ) " + ext);
        System.out.println("在Phoenix中执行的建表语句：" + createSql);
        PhoenixUtil.executeSql(createSql.toString());
    }


    //过滤掉不需要向下游传递的属性

    /**
     *
     * @param dataJSONobj
     * @param sinkColumns
     */
    //    //dataJsonObj:  {"tm_name":"三星3","logo_url":"/static/default.jpg","id":1}
    //    //sinkColumns:  id,tm_name
    private void filterColumn(JSONObject  dataJSONobj ,String sinkColumns){

        String[] columnArr = sinkColumns.split(",");
        List<String> list = Arrays.asList(columnArr);
        Set<Map.Entry<String, Object>> entries = dataJSONobj.entrySet();
        entries.removeIf(entry->!list.contains(entry.getKey()));
    }


}
