package com.hxqh.bigdata.ma.spark;

import com.hxqh.bigdata.ma.common.Constants;
import com.hxqh.bigdata.ma.util.StringUtils;
import org.apache.spark.AccumulatorParam;

import java.io.Serializable;

/**
 * Created by Ocean lin on 2018/1/15.
 */
public class CategoryAccumulator implements AccumulatorParam<String>, Serializable {

    @Override
    public String zero(String initialValue) {
        return Constants.CATEGORY_MANDARIN + "=0|"
                + Constants.CATEGORY_LOVE + "=0|"
                + Constants.CATEGORY_COMEDY + "=0|"
                + Constants.CATEGORY_EUROPE + "=0|"
                + Constants.CATEGORY_SCIENCE_FICTION + "=0|"
                + Constants.CATEGORY_FANTASY + "=0|"
                + Constants.CATEGORY_SUSPENSE + "=0|"
                + Constants.CATEGORY_USA + "=0|"
                + Constants.CATEGORY_GOOD_REPUTATION + "=0|"
                + Constants.CATEGORY_ACTION + "=0|"
                + Constants.CATEGORY_WARFARE + "=0|"
                + Constants.CATEGORY_ENGLISH + "=0|"
                + Constants.CATEGORY_CHINESE + "=0|"
                + Constants.CATEGORY_CINEMA + "=0|"
                + Constants.CATEGORY_THRILLER + "=0|"
                + Constants.CATEGORY_CRIME + "=0|"
                + Constants.CATEGORY_GUN_BATTLE + "=0";
    }

    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    /**
     * @param v1 连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }
        // 从v1中提取v2对应的值，并累加
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            int newValue = Integer.valueOf(oldValue) + 1;
            // 将v1中v2对应的值设置为累加的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }

}
