package com.holden.jdbc;

import java.sql.*;

/**
 * @ClassName stock_flink-jdbcTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月03日21:47 - 周五
 * @Describe
 */
public class jdbcTest {
    public static void main(String[] args) throws SQLException {
        String conStr = "jdbc:mysql://localhost:3306/spider_base?characterEncoding=utf-8&useSSL=false";
        // 2.建立连接
        Connection conn = DriverManager.getConnection(conStr, "root", "root");

        PreparedStatement ps = conn.prepareStatement("insert into employee values(7,1,1)");

        boolean execute = ps.execute();
        System.out.println(execute);
        conn.commit();



        ps.close();
        conn.close();

    }
}
