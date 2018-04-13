package com.hxqh.bigdata.ma.dao.impl;

import com.hxqh.bigdata.ma.conf.JDBCHelper;
import com.hxqh.bigdata.ma.dao.TaskDao;
import com.hxqh.bigdata.ma.model.Task;

/**
 * Created by Ocean lin on 2018/4/13.
 *
 * @author Ocean lin
 */
public class TaskDaoImpl implements TaskDao {
    @Override
    public void update(Task task) {
        String sql = "update task set task_status =? where taskid=? ";
        Object[] params = new Object[]{task.getTaskStatus(), task.getTaskid()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
