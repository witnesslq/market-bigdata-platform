package com.hxqh.bigdata.ma.dao.impl;

import com.hxqh.bigdata.ma.conf.JDBCHelper;
import com.hxqh.bigdata.ma.dao.TaskDao;
import com.hxqh.bigdata.ma.model.Task;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

    @Override
    public void updateWithExceptiom(Task task) {
        String sql = "update task set task_status=?,task_exception=? where taskid=? ";
        Object[] params = new Object[]{task.getTaskStatus(), task.getTaskDesc(), task.getTaskid()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }


    @Override
    public List<Task> findAll() {
        String sql = "SELECT taskid,task_param FROM task where task_status = 'undo' ";
        final List<Task> taskList = new ArrayList<>();
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    long taskId = rs.getLong("taskid");
                    String taskParam = rs.getString("task_param");
                    Task task = new Task();
                    task.setTaskid(taskId);
                    task.setTaskParam(taskParam);
                    taskList.add(task);
                }
            }

        });
        return taskList;
    }

    @Override
    public void updateStartTime(Date date, Long taskid) {
        String sql = "update task set  start_time =? where taskid=? ";
        Object[] params = new Object[]{date, taskid};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

    @Override
    public void updateFinishTime(Date date, Long taskid) {
        String sql = "update task set  finish_time =? where taskid=? ";
        Object[] params = new Object[]{date, taskid};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
