package com.wds.dataalgorithms.topN;

import com.wds.dataalgorithms.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * 使用takeOrdered的Spark Top 10 解决方案
 *
 * 假设：对于所有输入（K，V），K不是唯一的。
 * 这个类实现了Top N设计模式（N > 0）
 *
 * 对于输入（K，V）对，K非唯一，说明会看到类似（A，1）...（A，5）的输入数据
 *
 * 1、映射输入 =>（K，V）
 *
 * 2、归约（K，List<V1, V2, V3 ...Vn>） =>  (K, V），其中V=V1 + V2 + ... +Vn
 *
 * 3、使用takeOrdered找到Top N
 *
 * Created by wangdongsong1229@163.com on 2017/7/20.
 */
public class NoUniqueTopNTakeOrderSpark {

    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.err.println("Usage: SecondarySortSpark <Input-Path><topNum>");
            System.exit(1);
        }

        final String inputPath = args[0];
        final int N = Integer.parseInt(args[1]);

        JavaSparkContext ctx = SparkUtils.createJavaSparkContext("top-10-use-takeOrdered");

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);
        lines.saveAsTextFile("/output/1");

        JavaRDD<String> rdd = lines.coalesce(3);

        //映射（K，V）
        JavaPairRDD<String, Integer> kv = rdd.mapToPair((inputS) ->{
            String[] tokens = inputS.split(",");
            return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
        });
        kv.saveAsTextFile("/output/2");

        //归约
        JavaPairRDD<String, Integer> uniqueKeys = kv.reduceByKey((num1, num2) -> {
            return num1 + num2;
        });
        uniqueKeys.saveAsTextFile("/output/3");

        //takeOrdered and print result
        uniqueKeys.takeOrdered(N, (t1, t2) -> {
            return t1._1().compareTo(t2._1());
        }).forEach((entry) -> {
            System.out.println(entry._2 + "--" + entry._1);
        });

    }



}
