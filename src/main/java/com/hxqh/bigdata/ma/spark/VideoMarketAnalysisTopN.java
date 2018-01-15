package com.hxqh.bigdata.ma.spark;

import com.hxqh.bigdata.ma.util.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 统计Top5
 * <p>
 * Created by Ocean lin on 2018/1/12.
 */
public class VideoMarketAnalysisTopN {

    private static final String SPLIT_LABLE = "\\^";

    private static final String FILE_PATH = "E:\\";

    public static void main(String[] args) {

        final JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("VideoMarketAnalysisTopN").setMaster("local"));

        String filePath = FILE_PATH + DateUtils.getTodayDate();
        JavaRDD<String> stringJavaRDD = sc.textFile(filePath);
        JavaRDD<String> filterRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] split = v1.split(SPLIT_LABLE);
                if (split.length == 11) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        JavaPairRDD<Long, String> unSortedRDD = filterRDD.mapToPair(new PairFunction<String, Long, String>() {

            @Override
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] split = s.split(SPLIT_LABLE);
                Long playNum = Long.valueOf(split[10]);
                return new Tuple2<>(playNum, s);
            }
        });

        List<Tuple2<Long, String>> top5 = unSortedRDD.sortByKey(false).take(5);

        for (Tuple2<Long, String> tuple2 : top5) {
            System.out.println(tuple2._1 + ":" + tuple2._2);
        }
        sc.close();
    }
}
