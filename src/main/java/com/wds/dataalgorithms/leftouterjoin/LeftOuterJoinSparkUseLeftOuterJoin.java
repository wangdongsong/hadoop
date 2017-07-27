package com.wds.dataalgorithms.leftouterjoin;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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



    }
}