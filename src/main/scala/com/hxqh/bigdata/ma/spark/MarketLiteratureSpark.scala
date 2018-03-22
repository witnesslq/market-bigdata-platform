package com.hxqh.bigdata.ma.spark

import com.hxqh.bigdata.ma.common.Constants
import com.hxqh.bigdata.ma.util.{DateUtils, EsUtils}
import org.apache.spark.sql.SparkSession

/**
  * Created by Ocean lin on 2018/3/22.
  *
  * @author Ocean lin
  */
object MarketLiteratureSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("MarketLiteratureSpark").getOrCreate
    EsUtils.registerESTable(spark, "literature", "market_literature", "literature")
    // val startDate = DateUtils.getYesterdayDate();
    val startDate = "2018-03-01";
    val endDate = DateUtils.getTodayDate();

    val sql = "select * from literature where addTime>='" + startDate + "' and addTime<= '" + endDate + "'"
    val literature = spark.sql(sql).rdd
    literature.cache

    // [2018-03-20 17:15:56,风若叶,80000,61,连载 签约 VIP 都市 异术超能,都市频道,都市超品医圣,qidian,null,异术超能]
    // 点击量排名Top10
    literature.distinct().filter(e => if (e.get(2) == null) false else true).map(e => (e.getString(6), e.getLong(2))).reduceByKey(_ + _).map(e => (e._2, e._1))
      .sortByKey(false).take(Constants.LITERATURE_TOP_NUM).foreach(println(_))


    // 各标签占比情况
    literature.distinct().flatMap(e => {
      val splits = e.getString(4).split(" ")
      for (i <- 0 until splits.length - 1)
        yield (splits(i), 1)
    }).reduceByKey(_ + _).map(e => (e._2, e._1)).sortByKey(false).filter(e => if (e._1 > 10) true else false).collect().foreach(println(_))

    // 各标签点击量占比
    literature.distinct().filter(e => if (e.get(2) == null) false else true).flatMap(e => {
      val splits = e.getString(4).split(" ")
      for (i <- 0 until splits.length - 1)
        yield (splits(i), e.getLong(2))
    }).reduceByKey(_ + _).map(e => (e._2, e._1)).sortByKey(false).take(20).foreach(println(_))


    // 累计评论量最多作品排名Top10
    literature.distinct().filter(e => if (e.get(3) == null) false else true).map(e => (e.getString(6), e.getInt(3))).reduceByKey(_ + _).map(e => (e._2, e._1))
      .sortByKey(false).take(Constants.LITERATURE_TOP_NUM).foreach(println(_))


    // 累计评论量最多作者排名Top10
    literature.distinct().filter(e => if (e.get(3) == null) false else true).map(e => (e.getString(1), e.getInt(3))).reduceByKey(_ + _).map(e => (e._2, e._1))
      .sortByKey(false).take(Constants.LITERATURE_TOP_NUM).foreach(println(_))


    // 累计点击量最多作者排名Top10
    literature.distinct().filter(e => if (e.get(2) == null) false else true).map(e => (e.getString(1), e.getLong(2))).reduceByKey(_ + _).map(e => (e._2, e._1))
      .sortByKey(false).take(Constants.LITERATURE_TOP_NUM).foreach(println(_))

    // todo
    // 周度、月度各标签类别点击量变化曲线


  }
}
