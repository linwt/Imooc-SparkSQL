package com.imooc.web;

import com.imooc.dao.TopNDAO;
import com.imooc.domain.TopN;
import net.sf.json.JSONArray;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;


// 最受欢迎的TOPN课程
public class TopNServlet extends HttpServlet {

    private TopNDAO dao;

    @Override
    public void init() throws ServletException {
        dao = new TopNDAO();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String day = req.getParameter("day");
        List<TopN> results =  dao.query(day);
        JSONArray json = JSONArray.fromObject(results);
        resp.setContentType("text/html;charset=utf-8");
        PrintWriter writer = resp.getWriter();
        writer.println(json);
        writer.flush();
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req, resp);
    }
}

