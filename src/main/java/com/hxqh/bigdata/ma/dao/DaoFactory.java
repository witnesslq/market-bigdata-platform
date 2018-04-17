package com.hxqh.bigdata.ma.dao;

import com.hxqh.bigdata.ma.dao.impl.TaskDaoImpl;

/**
 * Created by Ocean lin on 2018/4/13.
 *
 * @author Ocean lin
 */
public class DaoFactory {
    public static TaskDao getTaskDAO() {
        return new TaskDaoImpl();
    }

}
