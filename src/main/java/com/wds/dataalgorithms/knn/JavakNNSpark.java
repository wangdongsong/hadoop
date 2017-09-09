package com.wds.dataalgorithms.knn;

import com.google.common.base.Splitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.xml.parsers.SAXParser;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * k-NN邻近算法
 * Created by wangdongsong1229@163.com on 2017/9/9.
 */
public class JavakNNSpark {

    public static void main(String[] args) {
        //Step2 处理输入参数

        //Step3 创建上下文

        //Step4 广播共享对象

        //Step5 对查询和训练数据集创建RDD

        //Step6 计算（R，S）的笛卡尔积

        //Step7 找出R中的r与S中的s之间的距离distance(r, s）

        //Step8 按R中的r对距离分组

        //Step9 找出k个近邻并对r分类
    }

    /**
     * 创建Spark上下文
     * @return
     * @throws Exception
     */
    private static JavaSparkContext createJavaSparkContext() throws Exception {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        return ctx;
    }

    /**
     *
     * @param str 是一个逗号或分号分隔的double值列表，例如"1.1, 2.2, 3.3"或"1.1; 2.2; 3.3"
     * @param delimiter 分隔符
     * @return
     */
    private static List<Double> splitOnToListOfDouble(String str, String delimiter) {
        Splitter splitter = Splitter.on(delimiter).trimResults();
        if (splitter.split(str) == null) {
            return null;
        }

        return StreamSupport.stream(splitter.split(str).spliterator(), false).map(Double::parseDouble).collect(Collectors.toList());
    }

}

