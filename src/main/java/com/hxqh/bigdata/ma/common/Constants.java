package com.hxqh.bigdata.ma.common;

/**
 * Created by Ocean lin on 2018/1/15.
 *
 * @author Lin
 */
public interface Constants {

    /**
     * Linux
     */
    String SPLIT_LABLE = "\\^";
    String FILE_PATH = "hdfs://spark1:9000/videos/";
    String FILE_SPLIT = "/";


//    /**
//     * Windows
//     */
//    String SPLIT_LABLE = "\\^";
//    String FILE_PATH = "E:\\";
//    String FILE_SPLIT = "\\";

    /**
     * 影视类别
     */
    String CATEGORY_MANDARIN = "国语";
    String CATEGORY_LOVE = "爱情";
    String CATEGORY_COMEDY = "喜剧";
    String CATEGORY_EUROPE = "欧洲";
    String CATEGORY_SCIENCE_FICTION = "科幻";
    String CATEGORY_FANTASY = "奇幻";
    String CATEGORY_SUSPENSE = "悬疑";
    String CATEGORY_USA = "美国";
    String CATEGORY_GOOD_REPUTATION = "口碑佳片";

    String CATEGORY_ACTION = "动作";
    String CATEGORY_WARFARE = "战争";
    String CATEGORY_ENGLISH = "英语";
    String CATEGORY_CHINESE = "华语";
    String CATEGORY_CINEMA = "院线";
    String CATEGORY_THRILLER = "惊悚";
    String CATEGORY_CRIME = "犯罪";
    String CATEGORY_GUN_BATTLE = "枪战";


    String HOST_SPARK3 = "spark3";
    Integer ES_PORT = 9300;
    String ES_PORT_STRING = "9200";

    String FILM_SPLIT_LABEL = ",";
    String FILM_SPLIT_SPACE = " ";


    Integer FILM_OFFSET_ACTOR = 9;
    Integer FILM_OFFSET_DIRECTOR = 3;
    Integer FILM_OFFSET_TITLE = 4;
    Integer FILM_OFFSET_PLAYNUM = 6;
    Integer FILM_OFFSET_SCORE = 7;
    Integer FILM_TOP_NUM = 10;


    String FILM_PLAYNUM_INDEX = "front_film_playnum";
    String FILM_LABEL_PIE_INDEX = "front_film_label_pie";
    String FILM_SCORE_NUM_INDEX = "front_film_tit1e_score";
    String FILM_COMPANY_INDEX = "front_film_tit1e_company";
    String FILM_ACTOR_PLAYNUM_INDEX = "front_film_actor_playnum";
    String FILM_ACTOR_SCORE_INDEX = "front_film_actor_score";
    String FILM_DIRECTOR_PLAYNUM_INDEX = "front_film_director_playnum";
    String FILM_DIRECTOR_SCORE_INDEX = "front_film_director_score";

    String FILM_PLAYNUM_TYPE = "film_playnum";
    String FILM_LABEL_PIE_TYPE = "label_pie";
    String FILM_SCORE_NUM_TYPE = "tit1e_score";
    String FILM_COMPANY_TYPE = "tit1e_company";
    String FILM_ACTOR_PLAYNUM_TYPE = "actor_playnum";
    String FILM_ACTOR_SCORE_TYPE = "actor_score";
    String FILM_DIRECTOR_PLAYNUM_TYPE = "director_playnum";
    String FILM_DIRECTOR_SCORE_TYPE = "director_score";


}
