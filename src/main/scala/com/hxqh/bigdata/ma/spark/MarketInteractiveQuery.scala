package com.hxqh.bigdata.ma.spark

import java.io.IOException
import java.util.Date

import com.alibaba.fastjson.JSON
import com.hxqh.bigdata.ma.common.Constants
import com.hxqh.bigdata.ma.dao.{DaoFactory, TaskDao}
import com.hxqh.bigdata.ma.domain.Show
import com.hxqh.bigdata.ma.model.Task
import com.hxqh.bigdata.ma.util.{DateUtils, ElasticSearchUtils, EsUtils}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by Ocean lin on 2018/4/8.
  *
  * @author Ocean lin
  */
object MarketInteractiveQuery extends Serializable {

  def main(args: Array[String]): Unit = {

    val client = ElasticSearchUtils.getClient
    val spark = SparkSession.builder.master("local").appName("MarketInteractiveQuery").getOrCreate


    val indexMap = Map(
      "soap" -> "film_data/film",
      "film" -> "film_data/film",
      "variety" -> "film_data/film",
      "maoyan" -> "maoyan/film",
      "book" -> "market_book2/book",
      "literature" -> "market_literature/literature"
    )

    while (true) {
      // 获取MySQL信息
      val taskDao = DaoFactory.getTaskDAO
      val taskList = taskDao.findAll()
      breakable {
        if (taskList.size() == 0) {
          Thread.sleep(5000)
          println("Spark作业监控中 " + (new Date()).toString)
          break
        } else {
          val task = taskList.get(0)
          val taskParameter: String = task.getTaskParam
          val taskJSON = JSON.parseObject(taskParameter)
          val startDate = taskJSON.getString(Constants.PARAM_START_DATE)
          val endDate = taskJSON.getString(Constants.PARAM_END_DATE)
          val category = taskJSON.getString(Constants.PARAM_CATEGORY)
          val title = taskJSON.getString(Constants.PARAM_TITLE)
          // 更新状态 正在运行
          persistStatus(task.getTaskid, Constants.RUNNING, taskDao)
          val indexName = indexMap(category).split("/")(0)
          val typeName = indexMap(category).split("/")(1)

          taskDao.updateStartTime(new Date(), task.getTaskid)

          // 电影、综艺、电视剧
          if (category.equals("film") || category.equals("variety") || category.equals("soap")) {
            EsUtils.registerESTable(spark, "film", indexName, typeName)
            val startSQL = "select playNum,addTime,label from film where"
            var categorySQL = " ";
            if (category.equals("film")) {
              categorySQL = categorySQL + "category ='film' "
            } else if (category.equals("variety")) {
              categorySQL = categorySQL + " category ='variety' "
            } else {
              categorySQL = categorySQL + " category ='soap' "
            }

            val titleFilter = " and filmName = '" + title + "' "
            val limitsSQL = " order by addtime desc limit 7"
            val commonSQL = " and addTime>='" + startDate + "' and addTime<= '" + endDate + "'"

            val sql = startSQL + categorySQL + titleFilter + commonSQL + limitsSQL
            val film = spark.sql(sql)
            // 写入ElasticSearch
            val filmRDD = film.rdd.collect()
            var label: String = null;
            filmRDD.foreach(e => {
              label = e.getString(2)
              val show = new Show(e.getInt(0).toDouble, e.get(1).toString, "line", task.getTaskid, "")
              addShow(show, client)
            })


            // 计算占比
            if (label.contains(" ")) {
              label = label.replace(" ", ",")
            }
            val labelMap = new mutable.HashMap[String, Int]()
            val filmLabels = label.split(",")
            for (element <- filmLabels) {
              labelMap.put(element, 1)
            }

            val sqlPie = startSQL + categorySQL + commonSQL
            val filmPieRDD = spark.sql(sqlPie).rdd
            val allPieRDD = filmPieRDD.flatMap(e => {
              var label = e.getString(2)
              if (label.contains(" "))
                label = label.replace(" ", ",")
              label.split(",")
            }).map((_, 1)).reduceByKey(_ + _).collect()


            allPieRDD.foreach(e => {
              if (labelMap.contains(e._1)) {
                val show = new Show(e._2.toDouble, null, "pie", task.getTaskid, e._1.toString)
                addShow(show, client)
              }
            })

            // 更新状态 完成
            persistStatus(task.getTaskid, Constants.FINISH, taskDao)
          }

          // 图书
          if (category.equals("book")) {
            EsUtils.registerESTable(spark, "book", indexName, typeName)
            val sql = "select commnetNum,addTime from book where addTime>='" + startDate + "' and addTime<= '" +
              endDate + "' and bookName = '" + title + "'   order by addtime desc limit 7"

            // 更新状态 完成
            persistStatus(task.getTaskid, Constants.FINISH, taskDao)
          }

          // 网络文学
          if (category.equals("literature")) {
            EsUtils.registerESTable(spark, "literature", indexName, typeName)
            val sql = "select clicknum,addtime from literature where addtime>='" + startDate + "' and addtime<= '" +
              endDate + "' and name = '" + title + "'   order by addtime desc limit 7"

            // 更新状态 完成
            persistStatus(task.getTaskid, Constants.FINISH, taskDao)
          }

          // 猫眼
          if (category.equals("maoyan")) {
            EsUtils.registerESTable(spark, "maoyan", indexName, typeName)
            val sql = "select boxInfo,addTime from maoyan where addTime>='" + startDate + "' and addTime<= '" +
              endDate + "' and filmName = '" + title + "'   order by addTime desc limit 7"

            // 更新状态 完成
            persistStatus(task.getTaskid, Constants.FINISH, taskDao)
          }


          taskDao.updateFinishTime(new Date(), task.getTaskid)

        } //end if
      }
    }
  }

  /**
    *
    * @param taskId 任务标识
    * @param status 任务状态
    */
  private def persistStatus(taskId: Any, status: String, taskDao: TaskDao) = {
    val task = new Task()
    task.setTaskid(taskId.asInstanceOf[Long])
    task.setTaskStatus(status)
    taskDao.update(task)
  }

  /**
    *
    * @param show   持久化show对象
    * @param client elasticsearch client
    */
  def addShow(show: Show, client: TransportClient): Unit = try {
    val todayTime = DateUtils.getTodayTime
    val content = XContentFactory.jsonBuilder.startObject.
      field("numvalue", show.numvalue).
      field("timeLine", show.name).
      field("category", show.category).
      field("taskid", show.taskid).
      field("filmName", show.filmName).
      field("addTime", todayTime).endObject

    client.prepareIndex(Constants.SEARCH_INDEX, Constants.SEARCH_TYPE).setSource(content).get
    println(show.filmName + " Persist to ES Success!")
  } catch {
    case e: IOException =>
      e.printStackTrace()
  }
}
