package com.hxqh.bigdata.ma.controller.spark;

import org.apache.spark.sql.SparkSession;

/**
 * Created by Ocean lin on 2018/2/28.
 *
 * @author Ocean lin
 */
public class StudentsHive {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("StudentsHive")
                .config("spark.sql.warehouse.dir", "/home/hadoop/file")
                .enableHiveSupport()
                .getOrCreate();

//        spark.sql("select * from students").show();
//        spark.sql("select count(*) from students").show();


        spark.sql("select * from test_load limit 10").show();
        spark.sql("select count(*) from test_load_2").show();

    }
}
