package com.wds.dataalgorithms.utils;

import org.apache.spark.SparkConf;
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

    public static JavaSparkContext createJavaSparkContext(String applicationName)
            throws Exception {
        SparkConf conf = new SparkConf().setAppName(applicationName);
        JavaSparkContext ctx = new JavaSparkContext(conf);
        return ctx;
    }

    public static String version() {
        return "2.0.0";
    }




}
