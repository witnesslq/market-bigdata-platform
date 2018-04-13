package com.hxqh.bigdata.ma.spark

import java.util.Properties
import java.util.concurrent.Executors

import com.alibaba.fastjson.JSON
import com.hxqh.bigdata.ma.common.Constants
import com.hxqh.bigdata.ma.conf.ConfigurationManager
import com.hxqh.bigdata.ma.dao.DaoFactory
import com.hxqh.bigdata.ma.model.Task
import com.hxqh.bigdata.ma.util.{EsUtils, ParamUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.Breaks._

/**
  * Created by Ocean lin on 2018/4/8.
  *
  * @author Ocean lin
  */
object MarketInteractiveQuery {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("MarketInteractiveQuery").getOrCreate
    val threadPool = Executors.newFixedThreadPool(1)

    val indexMap = Map(
      "soap" -> "film_data/film",
      "film" -> "film_data/film",
      "variety" -> "film_data/film",
      "book" -> "market_book2/book",
      "literature" -> "market_literature/literature"
    )

    while (true) {
      // 获取MySQL信息
      Thread.sleep(10000)
      val prop: Properties = new Properties
      prop.setProperty("user", ConfigurationManager.getProperty("spring.datasource.username"))
      prop.setProperty("password", ConfigurationManager.getProperty("spring.datasource.password"))
      val dataFrame: DataFrame = spark.read.jdbc(ConfigurationManager.getProperty("spring.datasource.url"),
        "task", Array[String]("task_status='" + Constants.UNDO + "'"), prop).select("taskid", "task_param")

      breakable {
        if (dataFrame.rdd.count() == 0) {
          break
        } else {
          threadPool.submit(new Runnable() {
            override def run(): Unit = {
              val taskId = dataFrame.javaRDD.take(1).get(0).get(0)
              val taskParameter = dataFrame.javaRDD.take(1).get(0).getString(1)
              val taskParam = JSON.parseObject(taskParameter)
              val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
              val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
              val category = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY)
              val title = ParamUtils.getParam(taskParam, Constants.PARAM_TITLE)


              // 更新状态 正在运行
              persistStatus(taskId, Constants.RUNNING)

              // 电影
              if (category.equals("film")) {
                val indexName = indexMap(category).split("/")(0)
                val typeName = indexMap(category).split("/")(1)
                EsUtils.registerESTable(spark, "film", indexName, typeName)
                val sql = "select filmName,playNum,addTime from film where addTime>='" + startDate + "' and addTime<= '" + endDate + "' and filmName = '" + title + "'    order by addtime desc limit 7"
                val film = spark.sql(sql)
                film.cache()
                val rdd = film.rdd
                rdd.foreach(println(_))
              }


              // 电视剧

              // 综艺

              // 图书

              // 网络文学

              // 猫眼


              // 更新状态 完成
              persistStatus(taskId, Constants.FINISH)
            }
          })
        } //end if
      }
    }
  }

  /**
    *
    * @param taskId 任务标识
    * @param status 任务状态
    */
  private def persistStatus(taskId: Any, status: String) = {
    val task = new Task()
    task.setTaskid(taskId.asInstanceOf[Long])
    task.setTaskStatus(status)
    val taskDao = DaoFactory.getTaskDAO
    taskDao.update(task)
  }
}
