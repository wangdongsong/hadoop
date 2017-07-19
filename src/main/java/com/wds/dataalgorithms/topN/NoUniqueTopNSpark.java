package com.wds.dataalgorithms.topN;

import com.wds.dataalgorithms.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Spark实现非唯一键
 * 思路：
 * 1、key不唯一，需要在计算前确保Key为一，将输入映射到JavaPairRDD<K,V>对，然后交给reduceByKey
 * 2、将所有唯一的（K、V）对划分为M个分区
 * 3、找出各分区的Top N，即本地top N
 * 4、找出所有本地的Top N的最终Top N
 * Created by wangdongsong1229@163.com on 2017/7/19.
 */
public class NoUniqueTopNSpark {

    public static void main(String[] args) throws Exception {
        //Setp 1、2 验证输入参数
        if (args.length < 1){
            System.err.println("Usage: SecondarySortSpark <file><topNum><direction:TOP|BOTTOM");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("args[0]: <file>=" + args[0]);
        int topN = 10;
        if (args[1] != null){
            topN = Integer.parseInt(args[1]);
        }
        //step3 创建上下文
        JavaSparkContext ctx = SparkUtils.createJavaSparkContext();
        //step4 广播Top N
        final Broadcast<Integer> topNBroadcase = ctx.broadcast(topN);
        //step5 从输入创建RDD
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);
        lines.saveAsTextFile("/output/1");
        //step6 RDD分区
        //一般经验执行器使用(2*num_executors*cores_per_executor)个分区
        JavaRDD<String> rdd = lines.coalesce(3);
        //step7 将输入（T)映射到（K，V）对
        JavaPairRDD<String, Integer> kv = rdd.mapToPair((s) ->{
            String[] tokens = s.split(",");
            return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
        });
        kv.saveAsTextFile("/output/2");

        //step8 归约
        JavaPairRDD<String, Integer> uniqueKey = kv.reduceByKey((s1, s2) -> {
            return s1 + s2;
        });
        uniqueKey.saveAsTextFile("/output/3");

        //step9 创建本地top N
        JavaRDD<SortedMap<Integer, String>> partitions = uniqueKey.mapPartitions((tuple2Iterator) -> {
            final int N = topNBroadcase.value();
            SortedMap<Integer, String> localTopN = new TreeMap<>();
            tuple2Iterator.forEachRemaining((t) -> {
                localTopN.put(t._2(), t._1());
                if (localTopN.size() > N) {
                    localTopN.remove(localTopN.firstKey());
                }
            });
            return Collections.singletonList(localTopN).iterator();
        });
        partitions.saveAsTextFile("/output/4");

        //step10 最终找到top N，也可以使用Reduce
        SortedMap<Integer, String> finalTopN = new TreeMap<>();
        List<SortedMap<Integer, String>> allTopN = partitions.collect();
        allTopN.forEach((map) -> {
            final int N = topNBroadcase.value();
            map.entrySet().forEach( (m) -> {
                finalTopN.put(m.getKey(), m.getValue());
            });

            if (finalTopN.size() > N) {
                finalTopN.remove(finalTopN.firstKey());
            }
        });



    }

}
