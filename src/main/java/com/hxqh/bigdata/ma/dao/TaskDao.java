package com.hxqh.bigdata.ma.dao;

import com.hxqh.bigdata.ma.model.Task;

/**
 * Created by Ocean lin on 2018/4/13.
 *
 * @author Ocean lin
 */
public interface TaskDao {

    /**
     * 更新task
     * @param task task任务实体类
     */
    void update(Task task);

}
