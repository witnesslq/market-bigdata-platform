package com.hxqh.bigdata.ma.repository;

import com.hxqh.bigdata.ma.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Ocean lin on 2018/4/8.
 *
 * @author Ocean lin
 */
@Repository
public interface TaskRepository extends JpaRepository<Task, Long> {
}
