package com.hxqh.bigdata.ma.controller.spark;

import com.hxqh.bigdata.ma.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by Ocean lin on 2018/3/15.
 *
 * @author Ocean lin
 */
public class MarketFilmSpark {


    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MarketFilmSpark")
                .getOrCreate();
        registerESTable(spark, "film", "film_data", "film");
        String startDate = "2018-03-14";
        String endDate = "2018-03-15";

        String sql = "select * from film  where addTime >='" + startDate + "' and addTime <= '" + endDate + "'";
        Dataset<Row> dataset = spark.sql(sql);
//        System.out.println(dataset.count());

        // 获取mysql中百度出品公司名称



    }


    /**
     * 获取ElasticSearch中的索引注册为表
     *
     * @param spark     SparkSession
     * @param tableName 临时表名称
     * @param indexName index名称
     * @param typeName type名称
     */
    private static void registerESTable(SparkSession spark, String tableName, String indexName, String typeName) {
        Map<String, String> esOptions = SparkUtil.initOption();
        Dataset<Row> dataset = spark.read().format("org.elasticsearch.spark.sql")
                .options(esOptions)
                .load(indexName + "/" + typeName);
        dataset.registerTempTable(tableName);
    }


}
