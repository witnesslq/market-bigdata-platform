package com.hxqh.bigdata.ma.controller.spark;

import com.hxqh.bigdata.ma.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by Ocean lin on 2018/3/13.
 *
 * @author Ocean lin
 */
public class ElasticSearchData {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ElasticSearchData")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        registerESTable(spark, "test");
        Dataset<Row> dataset = spark.sql("select * from test");

        System.out.println(dataset);
        System.out.println(dataset.count());
    }

    /**
     * 获取ElasticSearch中的索引注册为表
     *
     * @param spark
     * @param index
     */
    private static void registerESTable(SparkSession spark, String index) {
        Map<String, String> esOptions = SparkUtil.initOption();

        Dataset<Row> dataset = spark.read().format("org.elasticsearch.spark.sql")
                .options(esOptions)
                .load("film_data" + "/" + "film");
        dataset.registerTempTable(index);
    }


}
