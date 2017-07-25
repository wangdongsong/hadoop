package com.wds.dataalgorithms.leftouterjoin;

import org.apache.spark.api.java.JavaSparkContext;

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
    }
}
