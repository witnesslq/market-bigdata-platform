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

        spark.sql("select * from students").show();
        spark.sql("select name from students where score>=90").show();
        spark.sql("select name from students where age<=15").show();

    }
}
