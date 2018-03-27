package com.hxqh.bigdata.ma.spark

import java.io.IOException
import java.util
import java.util.Date

import com.hxqh.bigdata.ma.common.Constants
import com.hxqh.bigdata.ma.domain.Soap
import com.hxqh.bigdata.ma.util.{DateUtils, ElasticSearchUtils, SparkUtil}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory


/**
  * Created by Ocean lin on 2018/3/21.
  *
  * @author Ocean lin
  */
object MarketSoapSpark {

  def main(args: Array[String]): Unit = {
    //    val spark = SparkSession.builder.master("local").appName("MarketSoapSpark").getOrCreate
    val spark = SparkSession.builder.appName("MarketSoapSpark").getOrCreate
    registerESTable(spark, "film", "film_data", "film")
    val startDate = DateUtils.getYesterdayDate();
    val endDate = DateUtils.getTodayDate();

    val sql = "select * from Film  where  category = 'soap'  and addTime >='" + startDate + "' and addTime <= '" + endDate + "'"
    val soap = spark.sql(sql).rdd
    soap.cache
    val client = ElasticSearchUtils.getClient

    // [2018-03-21 09:58:39,soap,0,卫廉,城市传说,内地 科幻剧 普通话,7392000,0.0,iqiyi,莫芷涵 任茜贝 滕洋铖 王子月 杨旻浩 黄小熠,4324]
    // 播放量Top10
    val titlePlayNum = soap.distinct().map(e => (e.getInt(6), e.get(4))).sortByKey(false).take(Constants.SOAP_TOP_NUM)
    titlePlayNum.foreach(e => {
      val soap = new Soap(new Date(), e._1, e._2.toString)
      addSoap(soap, client, Constants.SOAP_PLAYNUM_INDEX, Constants.SOAP_PLAYNUM_TYPE)
    })

    // 分类占比
    soap.distinct().flatMap(e => (e.getString(5).split(" "))).map((_, 1)).
      reduceByKey(_ + _).filter(e => (e._2 > 10)).collect().foreach(x => {
      val soap = new Soap(new Date(), x._2.toDouble, x._1)
      addSoap(soap, client, Constants.SOAP_LABEL_PIE_INDEX, Constants.SOAP_LABEL_PIE_TYPE)
    })


    // 评论量Top10
    soap.distinct().map(e => ((e.getInt(10), e.getString(4)))).sortByKey(false).take(Constants.SOAP_TOP_NUM)
      .foreach(e => {
        val soap = new Soap(new Date(), e._1.toDouble, e._2)
        addSoap(soap, client, Constants.SOAP_SCORE_TITLE_INDEX, Constants.SOAP_SCORE_TITLE_TYPE)
      })


    // 播放量最多嘉宾Top10
    soap.distinct().filter(e => (null != e.get(9))).flatMap(e => {
      val splits = e.getString(9).split(" ")
      for (x <- 0 until splits.length - 1)
        yield (splits(x), e.getInt(6))
    }).reduceByKey(_ + _).map(e => (e._2, e._1)).sortByKey(false).take(Constants.SOAP_TOP_NUM).foreach(e => {
      val soap = new Soap(new Date(), e._1.toDouble, e._2)
      addSoap(soap, client, Constants.SOAP_GUEST_INDEX, Constants.SOAP_GUEST_TYPE)
    })


    // 评论量最高嘉宾Top10
    soap.distinct().filter(e => (null != e.get(9))).flatMap(e => {
      val splits = e.getString(9).split(" ")
      for (x <- 0 until splits.length - 1)
        yield (splits(x), e.getInt(10))
    }).reduceByKey(_ + _).map(e => (e._2, e._1)).sortByKey(false).take(Constants.SOAP_TOP_NUM).foreach(e => {
      val soap = new Soap(new Date(), e._1.toDouble, e._2)
      addSoap(soap, client, Constants.SOAP_GUEST_COMMENT_INDEX, Constants.SOAP_GUEST_COMMENT_TYPE)
    })

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

  /**
    *
    * @param soap      持久化的电视剧对象
    * @param client    elasticsearch client
    * @param indexName 索引名
    * @param typeName  类型名
    */
  def addSoap(soap: Soap, client: TransportClient, indexName: String, typeName: String): Unit = try {
    val todayTime = DateUtils.getTodayTime
    val content = XContentFactory.jsonBuilder.startObject.
      field("numvalue", soap.numvalue).
      field("name", soap.name).
      field("addTime", todayTime).endObject

    client.prepareIndex(indexName, typeName).setSource(content).get
    println(soap.name + " Persist to ES Success!")
  } catch {
    case e: IOException =>
      e.printStackTrace()
  }
}
