package com.hxqh.bigdata.ma.dao;

import com.hxqh.bigdata.ma.model.Task;

import java.util.Date;
import java.util.List;

/**
 * Created by Ocean lin on 2018/4/13.
 *
 * @author Ocean lin
 */
public interface TaskDao {

    /**
     * 更新task
     *
     * @param task task任务实体类
     */
    void update(Task task);

    /**
     * 更新task
     *
     * @param task task任务实体类及记录异常信息
     */
    void updateWithExceptiom(Task task);

    /**
     * 查询所有Task
     *
     * @return TaskList
     */
    List<Task> findAll();


    /**
     * 更新起始时间
     *
     * @param date 起始时间
     * @param taskid 任务编号
     */
    void updateStartTime(Date date, Long taskid);


    /**
     * 更新终止时间
     *
     * @param date 终止时间
     * @param taskid 任务编号
     */
    void updateFinishTime(Date date, Long taskid);

}
