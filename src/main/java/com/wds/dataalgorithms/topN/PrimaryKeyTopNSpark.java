package com.wds.dataalgorithms.topN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by wangdongsong1229@163.com on 2017/7/17.
 */
public class PrimaryKeyTopNSpark {

    public static void main(String[] args) {

        //Setp 1、2 验证输入参数
        if (args.length < 1){
            System.err.println("Usage: SecondarySortSpark <file>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("args[0]: <file>=" + args[0]);

        //Step3 连接SparkMaster
        SparkConf conf = new SparkConf();
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //Step4 读取源文件
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        //Step5 创建一组Tuple2(Key, Value)，一个参数为入参，后两个参数（K,V）是输出
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] tokens = s.split(",");
                return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
            }
        })
    }

}
