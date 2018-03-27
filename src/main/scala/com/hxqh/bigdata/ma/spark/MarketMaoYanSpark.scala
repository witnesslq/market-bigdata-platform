package com.hxqh.bigdata.ma.spark

import com.hxqh.bigdata.ma.common.Constants
import com.hxqh.bigdata.ma.util.{ElasticSearchUtils, EsUtils}
import org.apache.spark.sql.SparkSession

/**
  * Created by Ocean lin on 2018/3/27.
  *
  * @author Ocean lin
  */
object MarketMaoYanSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("MarketLiteratureSpark").getOrCreate
    EsUtils.registerESTable(spark, "maoyan", "maoyan", "film")
    //    val startDate = DateUtils.getYesterdayDate();
    //    val endDate = DateUtils.getTodayDate();


    val sql = "select * from maoyan where addTime = ( select max(addTime) from maoyan ) "

    val maoyan = spark.sql(sql).rdd
    maoyan.cache
    val client = ElasticSearchUtils.getClient


    // 新增时间、          累计分账票房 、  电影名称、 来源、上映时间、排片场次、累计综合票房、实时分账票房、实时综合票房
    // 2018-03-26 21:00:00,2977.35,环太平洋：雷霆再起,maoyan,上映4天,118940,2800.3,4.13,4.39

    // 累计分账票房占比
    maoyan.map(e => (e.getFloat(1), e.getString(2))).sortByKey(false).take(Constants.FILM_TOP_NUM).foreach(println(_))

    // 累计综合票房占
    maoyan.map(e => (e.getFloat(6), e.getString(2))).sortByKey(false).take(Constants.FILM_TOP_NUM).foreach(println(_))


    // 实时分账票房
    maoyan.map(e => (e.getFloat(7), e.getString(2))).sortByKey(false).take(Constants.FILM_TOP_NUM).foreach(println(_))

    // 实时综合票房
    maoyan.map(e => (e.getFloat(8), e.getString(2))).sortByKey(false).take(Constants.FILM_TOP_NUM).foreach(println(_))


    // 排片场次占比
    maoyan.map(e => (e.getLong(5), e.getString(2))).sortByKey(false).take(Constants.FILM_TOP_NUM).foreach(println(_))


    // todo
    // 豆瓣评分前10名

    // 各影片票房变化曲线
    // 各影片排片场次变化曲线


  }

}
