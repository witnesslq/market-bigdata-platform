package com.hxqh.bigdata.ma.service;

import com.alibaba.fastjson.JSONObject;
import com.hxqh.bigdata.ma.common.Constants;
import com.hxqh.bigdata.ma.model.Task;
import com.hxqh.bigdata.ma.repository.TaskRepository;
import com.hxqh.bigdata.ma.util.ParamUtils;
import com.hxqh.bigdata.ma.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Ocean lin on 2018/4/8.
 *
 * @author Ocean lin
 */
@Component
public class MarketInteractiveService implements Serializable {
    @Autowired
    private transient SparkSession spark;
    @Autowired
    private transient TaskRepository taskRepository;

    public void run() {
        List<Task> all = taskRepository.findAll();
        for (int i = 0; i < all.size(); i++) {
            Task task = all.get(i);
            System.out.println(task.getTaskParam());
        }

        String taskParam = all.get(0).getTaskParam();
        JSONObject jsonObject = JSONObject.parseObject(taskParam);
        String startDate = ParamUtils.getParam(jsonObject, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(jsonObject, Constants.PARAM_END_DATE);
        registerESTable(spark, "film", "film_data", "film");

        String sql = "select * from Film  where  category = 'film'  and addTime >='" + startDate + "' and addTime <= '" + endDate + "'";
        JavaRDD<Row> rowJavaRDD = spark.sql(sql).toJavaRDD();
        System.out.println(rowJavaRDD.count());
    }

    /**
     * 获取ElasticSearch中的索引注册为表
     *
     * @param spark     SparkSession
     * @param tableName 临时表名称
     * @param indexName index名称
     * @param typeName  type名称
     */
    private static void registerESTable(SparkSession spark, String tableName, String indexName, String typeName) {
        Map<String, String> esOptions = SparkUtil.initOption();
        Dataset<Row> dataset = spark.read().format("org.elasticsearch.spark.sql")
                .options(esOptions)
                .load(indexName + "/" + typeName);
        dataset.registerTempTable(tableName);
    }
}
