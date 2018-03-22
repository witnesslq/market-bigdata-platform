package com.hxqh.bigdata.ma.util

import java.util

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by Ocean lin on 2018/3/22.
  *
  * @author Ocean lin
  */
object EsUtils {
  /**
    * 获取ElasticSearch中的索引注册为表
    *
    * @param spark     SparkSession
    * @param tableName 临时表名称
    * @param indexName index名称
    * @param typeName  type名称
    */
  def registerESTable(spark: SparkSession, tableName: String, indexName: String, typeName: String): Unit = {
    val esOptions: util.Map[String, String] = SparkUtil.initOption
    val dataset: Dataset[Row] = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(indexName + "/" + typeName)
    dataset.createOrReplaceTempView(tableName)
  }
}
