package com.wds.dataalgorithms.mba;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MBA Spark分析解决方案
 * Created by wangdongsong1229@163.com on 2017/8/4.
 */
public class MBASpark {

    public static void main(String[] args) {
        //step2 处理参数
        //step3 创建Spark上下文
        //step4 从HDFS读取所有交易并创建第一个RDD
        //step5 生成频繁模式(map阶段）
        //step6 组合/归约模式(reduce阶段）
        //step7 生成所有子模式(map阶段2）
        //step8 生成关联规则（reduce阶段2)
    }

    /**
     * 创建Spark上下文对象
     *
     * @return
     * @throws Exception
     */
    static JavaSparkContext createJavaSparkContext() throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("market-baseket-analysis");
        //建立快速串行化器
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //32M缓存
        conf.set("spark.kryoserializer.buffer.mb", "32");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        return ctx;
    }

    /**
     * 转换为List
     * @param transaction
     * @return
     */
    static List<String> toList(String transaction) {
        String[] items = transaction.trim().split(",");
        return Stream.of(items).collect(Collectors.toList());
    }

    /**
     * 删除list中的第i个元素
     * @param list
     * @param i
     * @return
     */
    static List<String> removeOneItem(List<String> list, int i) {
        if (list == null || list.isEmpty()) {
            return list;
        }

        if (i < 0 || i > list.size() - 1) {
            return list;
        }
        List<String> cloned = new ArrayList<>(list);
        cloned.remove(i);
        return cloned;
    }



}
