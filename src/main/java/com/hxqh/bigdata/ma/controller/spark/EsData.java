package com.hxqh.bigdata.ma.controller.spark;

import com.hxqh.bigdata.ma.common.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Ocean lin on 2018/3/13.
 *
 * @author Ocean lin
 */
public class EsData {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("EsData")
                .getOrCreate();

        registerESTable(spark, "test");

        Dataset<Row> dataset = spark.sql("select * from test");
        System.out.println(dataset.count());

    }

    /**
     * 获取ElasticSearch中的索引注册为表
     * @param spark
     * @param index
     */
    private static void registerESTable(SparkSession spark, String index) {
        Map<String, String> esOptions = new HashMap<>(3);
        esOptions.put("es.nodes", Constants.HOST_SPARK3);
        esOptions.put("es.port", Constants.ES_PORT_STRING);
        esOptions.put("es.mapping.date.rich", "false");
        esOptions.put("es.index.auto.create", "true");

        Dataset<Row> dataset = spark.read().format("org.elasticsearch.spark.sql")
                .options(esOptions)
                .load("film_data" + "/" + "film");
        dataset.registerTempTable(index);
    }

}
