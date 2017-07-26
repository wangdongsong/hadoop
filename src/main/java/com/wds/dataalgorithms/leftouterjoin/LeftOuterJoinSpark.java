package com.wds.dataalgorithms.leftouterjoin;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import redis.clients.jedis.Tuple;
import scala.Tuple2;

import java.util.*;

/**
 * Created by wangdongsong1229@163.com on 2017/7/25.
 */
public class LeftOuterJoinSpark {
    public static void main(String[] args) {
        //setp2 读取输入参数
        if (args.length > 2){
            System.err.println("Usage: LeftOuterJoinSpark <users> <transactions>");
            System.exit(0);
        }
        String usersInputFile = args[0]; //输入文件
        String transactionsInputFile = args[1];
        System.out.println("users = " + usersInputFile);
        System.out.println("transactions = " + transactionsInputFile);

        //step3 创建JavaSparkContext对象
        JavaSparkContext ctx = new JavaSparkContext();

        //step4 为用户创建一个JavaRDD
        JavaRDD<String> users = ctx.textFile(usersInputFile, 1);
        JavaPairRDD<String, Tuple2<String, String>> userRDD = users.mapToPair((string) ->{
            String[] userRecord = string.split("\t");
            Tuple2<String, String> location = new Tuple2<>("L", userRecord[1]);
            return new Tuple2<String, Tuple2<String, String>>(userRecord[0], location);
        });

        //step5 为交易创建JavaRDD
        JavaRDD<String> transactions = ctx.textFile(transactionsInputFile, 1);
        JavaPairRDD<String, Tuple2<String, String>> transactionsRDD = transactions.mapToPair((string) -> {
            String[] transactionRecord = string.split("\t");
            Tuple2<String, String> product = new Tuple2<>("P", transactionRecord[1]);
            return new Tuple2<String, Tuple2<String, String>>(transactionRecord[2], product);
        });

        //step6 为step4和5生成的RDD创建一个并集
        JavaPairRDD<String, Tuple2<String, String>> allRDD = transactionsRDD.union(userRDD);

        //step7 groupByKey创建RDD
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRDD.groupByKey();

        //step8 创建一个JavaPairRDD作为productLocationsRDD
        JavaPairRDD<String, String> productLocationsRDD = groupedRDD.flatMapToPair((inputStr) -> {
            String location = "UNKNOWN";
            List<String> products = new ArrayList<String>();
            for (Tuple2<String, String> t : inputStr._2) {
                if (t._1().equalsIgnoreCase("L")) {
                    location = t._2();
                } else {
                    products.add(t._2());
                }
            }

            List<Tuple2<String, String>> kvList = new ArrayList<Tuple2<String, String>>();
            for (String p : products){
                kvList.add(new Tuple2<String, String>(p, location));
            }

            return kvList.iterator();
        });


        //step9 查找一个商品的所有地址
        JavaPairRDD<String, Iterable<String>> productByLocations = productLocationsRDD.groupByKey();

        //debug
        List<Tuple2<String, Iterable<String>>> debug = productByLocations.collect();
        System.out.println("-------debug begin--------");
        debug.forEach((t) -> {
            System.out.println("debug t._1 = " + t._1());
            System.out.println("debug t._2 = " + t._2());
        });

        //step10 改变值，对输出做最终处理
        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations = productByLocations.mapValues((iterable) -> {
            Set<String> uniqueLocations = new HashSet<>();
            for (String uniqueLocation : uniqueLocations) {
                uniqueLocations.add(uniqueLocation);
            }
            return new Tuple2<Set<String>, Integer>(uniqueLocations, uniqueLocations.size());
        });

        //step11 最终输出
        System.out.println("-----output-----");
        List<Tuple2<String, Tuple2<Set<String>, Integer>>> output = productByUniqueLocations.collect();
        output.forEach((t) -> {
            System.out.println("output t._1 = " + t._1());
            System.out.println("output t._2 = " + t._2());
        });
    }
}
