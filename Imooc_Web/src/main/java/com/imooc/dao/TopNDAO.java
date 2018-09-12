package com.imooc.dao;

import com.imooc.domain.TopN;
import com.imooc.utils.MySQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 面向接口编程
public class TopNDAO {

    static Map<String,String> courses = new HashMap<String,String>();
    static {
        courses.put("17891", "MySQL优化");
        courses.put("17892", "机器学习");
        courses.put("17893", "神经网络");
        courses.put("17894", "hadoop");
        courses.put("17895", "R语言");
        courses.put("17896", "java编程");
        courses.put("17897", "redis");
        courses.put("17898", "Swift");
        courses.put("17899", "Docker");
    }

    // 根据课程编号查询课程名称
    public String getCourseName(String id) {
        return courses.get(id);
    }

    // 根据day查询当天的最受欢迎的Top5课程
    public List<TopN> query(String day) {
        List<TopN> list = new ArrayList<TopN>();

        Connection connection = null;
        PreparedStatement state = null;
        ResultSet rs = null;

        try {
            connection = MySQLUtil.getConnection();
            String sql = "select courseId,times from day_top where day =? order by times desc limit 5";
            state = connection.prepareStatement(sql);
            state.setString(1, day);
            rs = state.executeQuery();
            TopN domain = null;
            while(rs.next()) {
                domain = new TopN();
                domain.setName(getCourseName(rs.getLong("courseId")+""));
                domain.setValue(rs.getLong("times"));
                list.add(domain);
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            MySQLUtil.release(connection, state, rs);
        }
        return list;
    }

    public static void main(String[] args) {
        TopNDAO dao = new TopNDAO();
        List<TopN> list = dao.query("2017-05-11");
        for(TopN result: list) {
            System.out.println(result.getName() + "-->" + result.getValue());
        }
    }

}

