package com.hxqh.bigdata.ma.util;

import com.hxqh.bigdata.ma.common.Constants;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

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

}
