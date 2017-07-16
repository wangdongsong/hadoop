package com.wds.dataalgorithms.secondarysort;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Chapter01 二次排序 Spark解决方案
 * Created by wangdongsong1229@163.com on 2017/7/15.
 */
public class SecondarySortSpark {

    public static void main(String[] args) {
        if (args.length < 1){
            System.err.println("Usage: SecondarySortSpark <file>");
            System.exit(1);
        }

        String inputPath = args[0];
        System.out.println("args[0]: <file>=" + args[0]);

        final JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                System.out.println("---DEBUG 01---");
                String[] tokens = s.split(",");
                System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
                Integer time = new Integer(tokens[1]);
                Integer value = new Integer(tokens[2]);
                Tuple2<Integer, Integer> timeValue = new Tuple2<>(time, value);
                return new Tuple2<>(tokens[0], timeValue);
            }
        });

        //生产环境禁用collect，因为影响性能，可在保存到文件中
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
        System.out.println("---DEBUG 02---");
        output.forEach((t) ->{
            Tuple2<Integer, Integer> timeValue = t._2();
            System.out.println(t._1 + ", " + timeValue._1 + ","  + timeValue._2);
        });

        //JavaPairRDD元素分组
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groups.collect();
        System.out.println("---DEBUG group by key---");
        output2.forEach((t) ->{
            Iterable<Tuple2<Integer, Integer>> list = t._2();
            System.out.println(t._1);
            list.forEach((l) ->{
                System.out.println(l._1 + "," + l._2);
            });
            System.out.println("===");
        });

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = groups.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {
            @Override
            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception {
                List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>(iterableToList(v1));
                Collections.sort(newList, SparkTupleComparator.INSTANCE);
                return newList;
            }
        });

        System.out.println("---DEBUG 03---");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 = sorted.collect();
        output3.forEach((t) ->{
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            list.forEach((t2) ->{
                System.out.println(t2._1 + "," + t2._2);
            });
            System.out.println("---------");
        });
    }

    static List<Tuple2<Integer,Integer>> iterableToList(Iterable<Tuple2<Integer,Integer>> iterable) {
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
        for (Tuple2<Integer,Integer> item : iterable) {
            list.add(item);
        }
        return list;
    }

}
