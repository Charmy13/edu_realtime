package com.atguigu.gmall.realtime.util;

import java.sql.*;

public class PhoenixUtil {
    public static void executeSql(String sql) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            //获取数据库操作对象
            conn = DruidDSutil.getConnection();
            //执行sql语句
           ps= conn.prepareStatement(sql);
           ps.execute();
//            ResultSet resultSet = ps.executeQuery();/
        } catch (SQLException e) {
           e.printStackTrace();
        }finally
        {
            if(ps!=null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }
}
