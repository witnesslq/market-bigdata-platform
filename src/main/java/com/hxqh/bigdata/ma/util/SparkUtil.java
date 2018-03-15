package com.hxqh.bigdata.ma.util;

import com.hxqh.bigdata.ma.common.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Ocean lin on 2018/3/15.
 *
 * @author Ocean lin
 */
public class SparkUtil {


    public static Map<String, String> initOption() {
        Map<String, String> esOptions = new HashMap<>(3);
        esOptions.put("es.nodes", Constants.HOST_SPARK3);
        esOptions.put("es.port", Constants.ES_PORT_STRING);
        esOptions.put("es.mapping.date.rich", "false");
        esOptions.put("es.index.auto.create", "true");
        return esOptions;
    }
}
