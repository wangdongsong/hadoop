package com.wds.dataalgorithms.leftouterjoin;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import redis.clients.jedis.Tuple;
import scala.Tuple2;

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


    }
}
