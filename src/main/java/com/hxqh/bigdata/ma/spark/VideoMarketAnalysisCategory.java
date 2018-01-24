package com.hxqh.bigdata.ma.spark;

import com.hxqh.bigdata.ma.common.Constants;
import com.hxqh.bigdata.ma.util.DateUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 各分类占比
 * <p>
 * Created by Ocean lin on 2018/1/12.
 */
public class VideoMarketAnalysisCategory {


    private static Integer VAL = 5;

    public static void main(String[] args) {

        final JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("VideoMarketAnalysisCategory").setMaster("local"));

        // 使用自定义计数器
        final Accumulator<String> categoryAccumulator = sc.accumulator("", new CategoryAccumulator());


        String filePath = Constants.FILE_PATH + DateUtils.getTodayDate();
        JavaRDD<String> stringJavaRDD = sc.textFile(filePath);

        JavaRDD<String> commonRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] split = v1.split(Constants.SPLIT_LABLE);
                if (split.length >= VAL) {
                    accumulator(categoryAccumulator, split[VAL]);
                    return true;
                } else {
                    return false;
                }
            }
        });


        JavaRDD<String> categoryRDD = commonRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(Constants.SPLIT_LABLE);
                String[] categoryLabel = split[VAL].split(" ");
                return Arrays.asList(categoryLabel);
            }
        });

        JavaRDD<String> category = categoryRDD.distinct();
        category.count();


        System.out.println(categoryAccumulator.value());
        // todo 持久化规则


        sc.close();
    }

    private static void accumulator(Accumulator<String> categoryAccumulator, String s) {
        String[] categoryLabel = s.split(" ");
        Map<String, Object> map = new HashMap<>(10);
        for (int i = 0; i < categoryLabel.length; i++) {
            map.put(categoryLabel[i], new Object());
        }

        if (map.containsKey(Constants.CATEGORY_MANDARIN)) {
            categoryAccumulator.add(Constants.CATEGORY_MANDARIN);
        } else if (map.containsKey(Constants.CATEGORY_LOVE)) {
            categoryAccumulator.add(Constants.CATEGORY_LOVE);
        } else if (map.containsKey(Constants.CATEGORY_COMEDY)) {
            categoryAccumulator.add(Constants.CATEGORY_COMEDY);
        } else if (map.containsKey(Constants.CATEGORY_EUROPE)) {
            categoryAccumulator.add(Constants.CATEGORY_EUROPE);
        } else if (map.containsKey(Constants.CATEGORY_SCIENCE_FICTION)) {
            categoryAccumulator.add(Constants.CATEGORY_SCIENCE_FICTION);
        } else if (map.containsKey(Constants.CATEGORY_FANTASY)) {
            categoryAccumulator.add(Constants.CATEGORY_FANTASY);
        } else if (map.containsKey(Constants.CATEGORY_SUSPENSE)) {
            categoryAccumulator.add(Constants.CATEGORY_SUSPENSE);
        } else if (map.containsKey(Constants.CATEGORY_USA)) {
            categoryAccumulator.add(Constants.CATEGORY_USA);
        } else if (map.containsKey(Constants.CATEGORY_GOOD_REPUTATION)) {
            categoryAccumulator.add(Constants.CATEGORY_GOOD_REPUTATION);
        } else if (map.containsKey(Constants.CATEGORY_ACTION)) {
            categoryAccumulator.add(Constants.CATEGORY_ACTION);
        } else if (map.containsKey(Constants.CATEGORY_WARFARE)) {
            categoryAccumulator.add(Constants.CATEGORY_WARFARE);
        } else if (map.containsKey(Constants.CATEGORY_ENGLISH)) {
            categoryAccumulator.add(Constants.CATEGORY_ENGLISH);
        } else if (map.containsKey(Constants.CATEGORY_CHINESE)) {
            categoryAccumulator.add(Constants.CATEGORY_CHINESE);
        } else if (map.containsKey(Constants.CATEGORY_CINEMA)) {
            categoryAccumulator.add(Constants.CATEGORY_CINEMA);
        } else if (map.containsKey(Constants.CATEGORY_THRILLER)) {
            categoryAccumulator.add(Constants.CATEGORY_THRILLER);
        } else if (map.containsKey(Constants.CATEGORY_CRIME)) {
            categoryAccumulator.add(Constants.CATEGORY_CRIME);
        } else if (map.containsKey(Constants.CATEGORY_GUN_BATTLE)) {
            categoryAccumulator.add(Constants.CATEGORY_GUN_BATTLE);
        }

    }

}
