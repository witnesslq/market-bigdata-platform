package com.hxqh.bigdata.ma.conf;

/**
 * 配置管理组件测试类
 *
 * @author Lin
 */
public class ConfigurationManagerTest {

    public static void main(String[] args) {
        String testkey1 = ConfigurationManager.getProperty("spring.datasource.username");
        String testkey2 = ConfigurationManager.getProperty("spring.datasource.password");
        System.out.println(testkey1);
        System.out.println(testkey2);
    }

}
