package com.wds.dataalgorithms.markovspark;

import com.wds.dataalgorithms.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by wangdongsong1229@163.com on 2017/9/4.
 */
public class MarkovSpark implements Serializable {

    public static void main(String[] args) {
        //Step1 处理输入参数
        if (args.length != 1) {
            System.err.println("Usage: MarkovSpark <input-path>");
            System.exit(0);
        }

        final String inputPath = args[0];
        System.out.println("inputPath:args[0]=" + args[0]);

        //Step2 创建上下文，并把输入转换为RDD
        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> records = ctx.textFile(inputPath, 1);

        //Step3 RDD转换为JavaPairRDD
        JavaPairRDD<String, Tuple2<Long, Integer>> kv = records.mapToPair((rec) ->{
            String[] tokens = StringUtils.split(rec, ",");
            if (tokens.length != 4) {
                return null;
            }
            long date = 0;
            date = DateUtil.getDateAsMilliSeconds(tokens[2]);
            int amout = Integer.parseInt(tokens[3]);
            Tuple2<Long, Integer> v = new Tuple2<>(date, amout);
            return new Tuple2<>(tokens[0], v);
        });


    }

}
