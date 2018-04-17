package com.hxqh.bigdata.ma.controller.spark;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by Ocean lin on 2018/3/13.
 *
 * @author Ocean lin
 */
public class ElasticSearchData {

    private static volatile Boolean isRunning = true;


    public static void main(String[] args) {

//        final SparkSession spark = SparkSession
//                .builder()
//                .master("local")
//                .appName("MarketFilmSpark")
//                .getOrCreate();
//        ElasticSearchUtils.registerESTable(spark, "film", "film_data", "film");
//        String startDate = "2018-03-27";
//        String endDate = DateUtils.getTodayDate();
//
//        String sql = "select * from Film  where  category = 'film'  and addTime >='" + startDate + "' and addTime <= '" + endDate + "'";
//        final Dataset<Row> film = spark.sql(sql);
//
//        System.out.println(film.count());


        //org.apache.commons.lang3.concurrent.BasicThreadFactory
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("example-schedule-pool-%d").daemon(true).build());

        int a = 1;
        for (; ; ) {
            if (a == 1) {
                System.out.println("do some");
                System.out.println("do some2");
            } else {
                System.out.println("do some");
                System.out.println("do some2");
                continue;
            }
        }

    }
}
