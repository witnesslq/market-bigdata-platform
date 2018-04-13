package com.hxqh.bigdata.ma.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;


/**
 * Created by Ocean lin on 2018/4/8.
 *
 * @author Ocean lin
 */
@Configuration
public class ApplicationConfig {

    @Autowired
    private Environment env;

//    @Value("${spark.app.name}")
//    private String appName;


    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("MarketFilmSpark")
                .setMaster("local")
                .set("spark.testing.memory", "2147480000");

        return sparkConf;
    }


    @Bean
    public SparkSession javaSparkContext() {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MarketFilmSpark").config(sparkConf())
                .getOrCreate();
        return spark;
    }

}
