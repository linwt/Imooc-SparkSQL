package com.imooc.utils;

import java.sql.*;

public class MySQLUtil {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";
    private static final String DRIVERCLASS = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/imooc";

    public static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName(DRIVERCLASS);
            con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

    public static void release(Connection con, PreparedStatement state, ResultSet rs) {
        if(rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(state != null) {
            try {
                state.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        System.out.print(getConnection());
    }
}
