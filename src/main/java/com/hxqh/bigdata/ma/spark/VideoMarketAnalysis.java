package com.hxqh.bigdata.ma.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by Ocean lin on 2018/1/12.
 */
public class VideoMarketAnalysis {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("VideoMarketAnalysis").setMaster("local"));

        JavaRDD<String> stringJavaRDD = sc.textFile("E:\\2018-01-12");
        JavaRDD<String> map = stringJavaRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1;
            }
        });
        long count = map.count();
        System.out.println(count);
        sc.close();
    }
}
