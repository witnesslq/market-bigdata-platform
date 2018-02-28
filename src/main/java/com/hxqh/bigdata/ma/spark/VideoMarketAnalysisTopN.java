package com.hxqh.bigdata.ma.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("VideoMarketAnalysisTopN").setMaster("local");
        // ES
        sparkConf.set("es.index.auto.create", "true");
        sparkConf.set("es.nodes", "spark3");
        sparkConf.set("es.port", "9200");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

//        String filePath = Constants.FILE_PATH + DateUtils.getTodayYear() +
//                Constants.FILE_SPLIT + DateUtils.getTodayMonth() + Constants.FILE_SPLIT;
//        JavaRDD<String> stringJavaRDD = sc.textFile();

        Dataset df = sqlContext.read().format("org.elasticsearch.spark.sql").load("market_analysis/videos");
        JavaRDD<Row> rowJavaRDD = df.toJavaRDD();

        // iqiyi^灵魂载体^崔航 苗诗钰 萨钢云 李未东 叶沛霖^吴焱晨^film^华语 惊悚 悬疑^6.7^298.0^73^2018-01-23^578000
        JavaPairRDD<Long, String> longStringJavaPairRDD = rowJavaRDD.mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                 long aLong = row.getLong(13);
                String string = row.getString(1);
                return new Tuple2<>(aLong,string);
            }
        });

//        JavaRDD<String> filterRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String v1) throws Exception {
//                String[] split = v1.split(Constants.SPLIT_LABLE);
//                if (split.length == PALY_NUM_LOC + 1 && !split[TALK_NUM_LOC].equals("评分人数不足")) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });


        /***************************播放量排名*******************************/
//        JavaPairRDD<Long, String> palyNumRDD = filterRDD.mapToPair(new PairFunction<String, Long, String>() {
//
//            @Override
//            public Tuple2<Long, String> call(String s) throws Exception {
//                String[] split = s.split(Constants.SPLIT_LABLE);
//                Long playNum = Long.valueOf(split[PALY_NUM_LOC]);
//                return new Tuple2<>(playNum, s);
//            }
//        });
        List<Tuple2<Long, String>> playNumTop = longStringJavaPairRDD.sortByKey(false).take(TOP_NUM);
        for (Tuple2<Long, String> tuple2 : playNumTop) {
            System.out.println(tuple2._1 + ":" + tuple2._2);
        }
        // todo 持久化及格式
        /***************************播放量排名*******************************/


        /***************************评论量排名*******************************/
//        JavaPairRDD<Long, String> commentNumRDD = filterRDD.mapToPair(new PairFunction<String, Long, String>() {
//            @Override
//            public Tuple2<Long, String> call(String s) throws Exception {
//                String[] split = s.split(Constants.SPLIT_LABLE);
//                Long commentNum = Double.valueOf(split[TALK_NUM_LOC]).longValue();
//                return new Tuple2<>(commentNum, s);
//            }
//        });
//        List<Tuple2<Long, String>> commentNumTop = commentNumRDD.sortByKey(false).take(TOP_NUM);
//
//        for (Tuple2<Long, String> tuple2 : commentNumTop) {
//            System.out.println(tuple2._1 + ":" + tuple2._2);
//        }
        // todo 持久化及格式
        /***************************评论量排名*******************************/


        sc.close();
    }
}
