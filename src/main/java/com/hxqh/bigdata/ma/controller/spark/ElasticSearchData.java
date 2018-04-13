package com.hxqh.bigdata.ma.controller.spark;

import com.hxqh.bigdata.ma.util.DateUtils;
import com.hxqh.bigdata.ma.util.ElasticSearchUtils;
import com.hxqh.bigdata.ma.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by Ocean lin on 2018/3/13.
 *
 * @author Ocean lin
 */
public class ElasticSearchData {

    public static void main(String[] args) {

        final SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MarketFilmSpark")
                .getOrCreate();
        ElasticSearchUtils.registerESTable(spark, "film", "film_data", "film");
        String startDate = "2018-03-27";
        String endDate = DateUtils.getTodayDate();

        String sql = "select * from Film  where  category = 'film'  and addTime >='" + startDate + "' and addTime <= '" + endDate + "'";
        final Dataset<Row> film = spark.sql(sql);

        System.out.println(film.count());
    }



}
