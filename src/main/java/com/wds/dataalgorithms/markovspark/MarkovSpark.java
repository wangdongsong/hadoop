package com.wds.dataalgorithms.markovspark;

import com.wds.dataalgorithms.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
        kv.saveAsTextFile("/output/markov/output/3");

        //Step4 按CustomerID对交易分组
        JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> customerRDD = kv.groupByKey();
        customerRDD.saveAsTextFile("/output/markov/output/4");

        //Step5 创建马尔可夫状态序列
        JavaPairRDD<String, List<String>> stateSequence = customerRDD.mapValues((Iterable<Tuple2<Long, Integer>> dateAndAmout) -> {
            List<Tuple2<Long, Integer>> list = toList(dateAndAmout);
            Collections.sort(list, TupleComparatorAscending.INSTANCE);
            return toStateSequence(list);
        });
    }

    private static <U> U toStateSequence(List<Tuple2<Long, Integer>> list) {
        return null;
    }

    private static List<Tuple2<Long, Integer>> toList(Iterable<Tuple2<Long, Integer>> dateAndAmout) {
        return null;
    }

    static class TupleComparatorAscending implements Comparator<Tuple2<Long, Integer>>, Serializable {
        final static TupleComparatorAscending INSTANCE = new TupleComparatorAscending();
        @Override
        public int compare(Tuple2<Long, Integer> t1, Tuple2<Long, Integer> t2) {
            // return -t1._1.compareTo(t2._1);     // sorts RDD elements descending
            return t1._1.compareTo(t2._1);         // sorts RDD elements ascending
        }
    }

}
