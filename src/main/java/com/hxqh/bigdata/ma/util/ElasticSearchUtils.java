package com.hxqh.bigdata.ma.util;

import com.hxqh.bigdata.ma.common.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Map;

/**
 * Created by Ocean lin on 2018/3/19.
 *
 * @author Ocean lin
 */
public class ElasticSearchUtils {

    public static TransportClient getClient() {
        TransportClient client = null;
        try {
            Settings settings = Settings.builder()
                    .put("client.transport.sniff", true)
                    .put("cluster.name", "es-market-analysis").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(Constants.HOST_SPARK3), Constants.ES_PORT));
        } catch (Exception ex) {
            client.close();
        } finally {
        }
        return client;
    }

    /**
     * 获取ElasticSearch中的索引注册为表
     *
     * @param spark     SparkSession
     * @param tableName 临时表名称
     * @param indexName index名称
     * @param typeName  type名称
     */
    public static void registerESTable(SparkSession spark, String tableName, String indexName, String typeName) {
        Map<String, String> esOptions = SparkUtil.initOption();
        Dataset<Row> dataset = spark.read().format("org.elasticsearch.spark.sql")
                .options(esOptions)
                .load(indexName + "/" + typeName);
        dataset.registerTempTable(tableName);
    }

}
