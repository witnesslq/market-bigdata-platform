package com.hxqh.bigdata.ma.spark;

import com.hxqh.bigdata.ma.common.Constants;
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


    private static final Integer TOP_NUM = 10;

    private static final Integer PALY_NUM_LOC = 10;
    private static final Integer TALK_NUM_LOC = 7;

    public static void main(String[] args) {
        final JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("VideoMarketAnalysisTopN").setMaster("local"));

        String filePath = Constants.FILE_PATH + DateUtils.getTodayYear() +
                Constants.FILE_SPLIT + DateUtils.getTodayMonth() + Constants.FILE_SPLIT;
        JavaRDD<String> stringJavaRDD = sc.textFile(filePath);
        JavaRDD<String> filterRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] split = v1.split(Constants.SPLIT_LABLE);
                if (split.length == PALY_NUM_LOC + 1 && !split[TALK_NUM_LOC].equals("评分人数不足")) {
                    return true;
                } else {
                    return false;
                }
            }
        });


        /***************************播放量排名*******************************/
        JavaPairRDD<Long, String> palyNumRDD = filterRDD.mapToPair(new PairFunction<String, Long, String>() {

            @Override
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] split = s.split(Constants.SPLIT_LABLE);
                Long playNum = Long.valueOf(split[PALY_NUM_LOC]);
                return new Tuple2<>(playNum, s);
            }
        });
        List<Tuple2<Long, String>> playNumTop = palyNumRDD.sortByKey(false).take(TOP_NUM);
        for (Tuple2<Long, String> tuple2 : playNumTop) {
            System.out.println(tuple2._1 + ":" + tuple2._2);
        }
        // todo 持久化及格式
        /***************************播放量排名*******************************/


        /***************************评论量排名*******************************/
        JavaPairRDD<Long, String> commentNumRDD = filterRDD.mapToPair(new PairFunction<String, Long, String>() {
            @Override
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] split = s.split(Constants.SPLIT_LABLE);
                Long commentNum = Double.valueOf(split[TALK_NUM_LOC]).longValue();
                return new Tuple2<>(commentNum, s);
            }
        });
        List<Tuple2<Long, String>> commentNumTop = commentNumRDD.sortByKey(false).take(TOP_NUM);

        for (Tuple2<Long, String> tuple2 : commentNumTop) {
            System.out.println(tuple2._1 + ":" + tuple2._2);
        }
        // todo 持久化及格式
        /***************************评论量排名*******************************/


        sc.close();
    }
}
