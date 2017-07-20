package com.wds.dataalgorithms.utils;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wangdongsong1229@163.com on 2017/7/19.
 */
public class SparkUtils {

    public static JavaSparkContext createJavaSparkContext()
            throws Exception {
        return new JavaSparkContext();
    }

    public static JavaSparkContext createJavaSparkContext(String sparkMasterURL, String applicationName)
            throws Exception {
        JavaSparkContext ctx = new JavaSparkContext(sparkMasterURL, applicationName);
        return ctx;
    }

}
