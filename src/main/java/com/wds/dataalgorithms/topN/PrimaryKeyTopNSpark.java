package com.wds.dataalgorithms.topN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Stream;

/**
 * 唯一键，Spark，不使用take
 * Created by wangdongsong1229@163.com on 2017/7/17.
 */
public class PrimaryKeyTopNSpark {

    public static void main(String[] args) {

        //Setp 1、2 验证输入参数
        if (args.length < 1){
            System.err.println("Usage: SecondarySortSpark <file><topNum><direction:TOP|BOTTOM");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("args[0]: <file>=" + args[0]);

        //默认Top10
        String direction = "TOP";
        int topN = 10;
        if (args[1] != null){
            topN = Integer.parseInt(args[1]);
        }
        if (args[2] != null && args[2].equalsIgnoreCase("BOTTOM")) {
            direction = "BOTTOM";
        }

        //Step3 连接SparkMaster
        SparkConf conf = new SparkConf();
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //设置TopN的参数，Broadcase可以广播，所有节点都可以收到
        final Broadcast<Integer> broadcastTopN = ctx.broadcast(topN);
        final Broadcast<String> broadcastDirection = ctx.broadcast(direction);

        //Step4 读取源文件
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        //Step5 创建一组Tuple2(Key, Value)，一个参数为入参，后两个参数（K,V）是输出
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] tokens = s.split(",");
                return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
            }
        });

        //Step5 函数式写法
        pairs = lines.mapToPair((s) ->{
            String[] tokens = s.split(",");
            return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
        });

        //step6 为各个输入分区创建本地的Top N列表
        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Integer> tuple = tuple2Iterator.next();
                    top10.put(tuple._2, tuple._1);
                    //使用设置参数
                    setup(broadcastTopN, broadcastDirection, top10);
                }

                return Collections.singletonList(top10).iterator();
            }

        });

        //step7 使用collect创建最终的top 10列表，该步有替换方案，使用JavaRDD.reduce
        SortedMap<Integer, String> finalTop10 = new TreeMap<>();
        List<SortedMap<Integer, String>> list = partitions.collect();
        list.forEach((l) ->{
            mergeMap(broadcastTopN, broadcastDirection, l, finalTop10);
        });

        //Step7的替换方案，使用JavaRDD.reduce
        SortedMap<Integer, String> finalTop10Reduce = partitions.reduce((m1, m2) -> {
            SortedMap<Integer, String> top10 = new TreeMap<>();
            mergeMap(broadcastTopN, broadcastDirection, m1, top10);
            mergeMap(broadcastTopN, broadcastDirection, m2, top10);
            return top10;
        });

        //step8 输出最终结果
        finalTop10.forEach((key, value) ->{
            System.out.println(key + ", " + value);
        });
        System.out.println("---Reduce---");
        finalTop10Reduce.forEach((key, value) ->{
            System.out.println(key + ", " + value);
        });

    }

    private static void mergeMap(Broadcast<Integer> broadcastTopN, Broadcast<String> broadcastDirection, SortedMap<Integer, String> map, SortedMap<Integer, String> top10) {
        map.entrySet().forEach((m) ->{
            top10.put(m.getKey(), m.getValue());
            setup(broadcastTopN, broadcastDirection, top10);
        });
    }

    private static void setup(Broadcast<Integer> broadcastTopN, Broadcast<String> broadcastDirection, SortedMap<Integer, String> top10) {
        if (top10.size() > broadcastTopN.getValue()) {
            if (broadcastDirection.getValue().equalsIgnoreCase("TOP")) {
                top10.remove(top10.firstKey());
            } else {
                top10.remove(top10.lastKey());
            }
        }
    }

}
