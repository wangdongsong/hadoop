package com.wds.dataalgorithms.leftouterjoin;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import scala.Option;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.function.Function;

/**
 * 使用LeftOuterJoin的Spark实现，MapReduce没有提供类似的方法
 *
 * 使用LeftOuterJoin方法可以避免以下问题：
 *
 * 1、对users和transactions使用JavaPairRDD.union操作，开销大
 * 2、引入定制标志，如地址增加"L"，为商品增加“P”
 * 3、使用额外的RDD转换来区分定制标志
 *
 * Created by wangdongsong1229@163.com on 2017/7/27.
 */
public class LeftOuterJoinSparkUseLeftOuterJoin {
    public static void main(String[] args) {
        //setp2 读取输入参数
        if (args.length > 2){
            System.err.println("Usage: LeftOuterJoinSparkUseLeftOuterJoin <users> <transactions>");
            System.exit(0);
        }
        String usersInputFile = args[0]; //输入文件
        String transactionsInputFile = args[1];
        System.out.println("users = " + usersInputFile);
        System.out.println("transactions = " + transactionsInputFile);

        //step3 创建JavaSparkContext对象
        JavaSparkContext ctx = new JavaSparkContext();

        //step4 为用户数据创建RDD
        JavaRDD<String> users = ctx.textFile(usersInputFile, 1);

        //step5 创建userRDD-右表
        JavaPairRDD<String, String> usersRDD = users.mapToPair((s) -> {
            String[] userRecord = s.split("\t");
            return new Tuple2<String, String>(userRecord[0], userRecord[1]);
        });

        //step6 为交易数据创建一个RDD
        JavaRDD<String> transactions = ctx.textFile(transactionsInputFile, 1);

        //step7 创建transactionsRDD - 左表
        JavaPairRDD<String, String> transactionsRDD = transactions.mapToPair((s) -> {
            String[] transactionRecord = s.split("\t");
            return new Tuple2<String, String>(transactionRecord[2], transactionRecord[1]);
        });

        //step8 使用内置leftOuterJoin方法
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joined = transactionsRDD.leftOuterJoin(usersRDD);
        joined.saveAsTextFile("/output/leftjoin/1");

        //step9 创建(product, location)对
        JavaPairRDD<String, String> products = joined.mapToPair((t) -> {
            Tuple2<String, Optional<String>> value = t._2();
            return new Tuple2<>(value._1, value._2.get());
        });
        products.saveAsTextFile("/output/leftjoin/2");

        //step10 按键完成分组(K = product, V = location)对的分组
        JavaPairRDD<String, Iterable<String>> productByLocations = products.groupByKey();
        productByLocations.saveAsTextFile("/output/leftjoin/3");

        //setp11 创建最终输出
        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocatoins = productByLocations.mapValues((iter) -> {
            Set<String> uniqueLocations = new HashSet<String>();
            iter.forEach((t) -> {
                uniqueLocations.add(t);
            });
            return new Tuple2<>(uniqueLocations, uniqueLocations.size());
        });
        productByUniqueLocatoins.saveAsTextFile("/output/leftjoin/4");


        /*
         * 合并步骤10和11
         */
        Function<String, Set<String>> createCombiner = new Function<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                Set<String> set = new HashSet<>();
                set.add(s);
                return set;
            }
        };

        Function2<Set<String>, String, Set<String>> mergeValue = new Function2<Set<String>, String, Set<String>>() {
            @Override
            public Set<String> call(Set<String> v1, String v2) throws Exception {
                v1.add(v2);
                return v1;
            }
        };

        Function2<Set<String>, Set<String>, Set<String>> mergeCombiners = new Function2<Set<String>, Set<String>, Set<String>>() {
            @Override
            public Set<String> call(Set<String> v1, Set<String> v2) throws Exception {
                v1.addAll(v2);
                return v1;
            }
        };

        JavaPairRDD<String, Set<String>> productuniqueLocations2 = products.combineByKey(createCombiner,mergeValue, mergeCombiners);


        //最终输出
        Map<String, Set<String>> productMap = productuniqueLocations2.collectAsMap();
        productMap.entrySet().forEach((entry) -> {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        });
    }


}
