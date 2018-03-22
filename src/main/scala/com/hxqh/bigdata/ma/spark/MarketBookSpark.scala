package com.hxqh.bigdata.ma.spark

import java.util

import com.hxqh.bigdata.ma.common.Constants
import com.hxqh.bigdata.ma.util.{DateUtils, ElasticSearchUtils, SparkUtil}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by Ocean lin on 2018/3/22.
  *
  * @author Ocean lin
  */
object MarketBookSpark {

  def main(args: Array[String]): Unit = {
    //    val spark = SparkSession.builder.master("local").appName("MarketBookSpark").getOrCreate
    val spark = SparkSession.builder.appName("MarketBookSpark").getOrCreate
    registerESTable(spark, "book", "market_book2", "book")
    val startDate = DateUtils.getYesterdayDate();
    val endDate = DateUtils.getTodayDate();

    val sql = "select * from book where addTime>='" + startDate + "' and addTime<= '" + endDate + "'"
    val variety = spark.sql(sql).rdd
    variety.cache

    // 2018-03-21 18:01:07,黄成明,数据化管理：洞悉零售及电子商务运营,计算机与互联网,计算机与互联网 电子商务 Broadview 数据化管理：洞悉零售及电子商务运营,6700,42.2,电子工业出版社,jd


    // 累计评论量排名Top10
    variety.distinct().map(e => (e.getString(2), e.getLong(5))).reduceByKey(_ + _).map(e => (e._2, e._1)).
      sortByKey(false).take(Constants.BOOK_TOP_NUM).foreach(println(_))

    // 各类别占比情况
    variety.distinct().flatMap(e => {
      val splits = e.getString(4).split(" ")
      for (i <- 0 until splits.length)
        yield (splits(i), 1)
    }).reduceByKey(_ + _).map(e => (e._2, e._1)).sortByKey(false).take(10).foreach(println(_))

    // 累计评论量最多出版社排名Top10
    variety.distinct().map(e => (e.getString(7), e.getLong(5))).reduceByKey(_ + _).map(e => (e._2, e._1)).
      sortByKey(false).take(Constants.BOOK_TOP_NUM).foreach(println(_))

    // todo
    // 豆瓣评分Top10
    // 豆瓣评分人数Top10


    val client = ElasticSearchUtils.getClient
  }

  /**
    * 获取ElasticSearch中的索引注册为表
    *
    * @param spark     SparkSession
    * @param tableName 临时表名称
    * @param indexName index名称
    * @param typeName  type名称
    */
  private def registerESTable(spark: SparkSession, tableName: String, indexName: String, typeName: String): Unit = {
    val esOptions: util.Map[String, String] = SparkUtil.initOption
    val dataset: Dataset[Row] = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load(indexName + "/" + typeName)
    dataset.createOrReplaceTempView(tableName)
  }

}
