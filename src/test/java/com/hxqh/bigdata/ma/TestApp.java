package com.hxqh.bigdata.ma;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Ocean lin on 2018/1/15.
 */
public class TestApp {

    @Test
    public void testSplit() {
        String s = "爱奇艺^巨额来电^陈学冬 桂纶镁 张孝全 黄由启 蒋梦婕^彭顺^电影^华语 惊悚 悬疑^7.5^207000.0^4215^2018-01-15^18399000";
        String[] split = s.split("\\^");
        Assert.assertEquals(11, split.length);
    }
}
