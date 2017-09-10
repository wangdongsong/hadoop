package com.wds.dataalgorithms.knn;

import com.google.common.base.Splitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import javax.xml.parsers.SAXParser;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * k-NN邻近算法
 * Created by wangdongsong1229@163.com on 2017/9/9.
 */
public class JavakNNSpark {

    public static void main(String[] args) throws Exception {
        //Step2 处理输入参数
        if (args.length < 4) {
            System.err.println("Usage: kNN <k-knn> <d-dimension> <R> <S>");
            System.exit(0);
        }
        Integer k = Integer.valueOf(args[0]);
        Integer d = Integer.valueOf(args[1]);
        String dataSetR = args[2];
        String dataSetS = args[3];

        //Step3 创建上下文
        JavaSparkContext ctx = createJavaSparkContext();

        //Step4 广播共享对象
        //为了能够从集群节点访问共享对象和数据结构，可以使用Broadcast类。
        final Broadcast<Integer> boardcaseK = ctx.broadcast(k);
        final Broadcast<Integer> broadcastD = ctx.broadcast(d);

        //Step5 对查询和训练数据集创建RDD
        JavaRDD<String> R = ctx.textFile(dataSetR, 1);
        R.saveAsTextFile("/output/knn/output/R");

        JavaRDD<String> S = ctx.textFile(dataSetS, 1);
        S.saveAsTextFile("/output/knn/output/S");

        //Step6 计算（R，S）的笛卡尔积
        JavaPairRDD<String, String> cart = R.cartesian(S);
        cart.saveAsTextFile("/output/knn/output/cart");

        //Step7 找出R中的r与S中的s之间的距离distance(r, s）
        JavaPairRDD<String, Tuple2<Double, String>> knnMapped = cart.mapToPair((cartRecord) -> {
            String rRecord = cartRecord._1();
            String sRecord = cartRecord._2();
            String[] rTokens = rRecord.split(";");
            String rRecordID = rTokens[0];
            String r = rTokens[1];
            String[] sTokens = sRecord.split(";");
            String sClassificationID = sTokens[1];
            String s = sTokens[2];
            Integer d1 = broadcastD.value();
            double distance = calculateDistance(r, s, d1);
            String K = rRecordID;
            Tuple2<Double, String> V = new Tuple2<>(distance, sClassificationID);
            return new Tuple2<String, Tuple2<Double, String>>(K, V);
        });

        //Step8 按R中的r对距离分组

        //Step9 找出k个近邻并对r分类
    }

    /**
     * 创建Spark上下文
     * @return
     * @throws Exception
     */
    private static JavaSparkContext createJavaSparkContext() throws Exception {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        return ctx;
    }

    /**
     *
     * @param str 是一个逗号或分号分隔的double值列表，例如"1.1, 2.2, 3.3"或"1.1; 2.2; 3.3"
     * @param delimiter 分隔符
     * @return
     */
    private static List<Double> splitOnToListOfDouble(String str, String delimiter) {
        Splitter splitter = Splitter.on(delimiter).trimResults();
        if (splitter.split(str) == null) {
            return null;
        }

        return StreamSupport.stream(splitter.split(str).spliterator(), false).map(Double::parseDouble).collect(Collectors.toList());
    }

    /**
     * 接受两个向量R和S，计算它们之间的欧氏距离
     *
     * @param rAsString
     * @param sAsString
     * @param d
     * @return
     */
    private static double calculateDistance(String rAsString, String sAsString, int d) {
        List<Double> r = splitOnToListOfDouble(rAsString, ",");
        List<Double> s = splitOnToListOfDouble(sAsString, ",");
        if (r.size() != d) {
            return Double.NaN;
        }

        if (s.size() != d) {
            return Double.NaN;
        }

        double sum = 0.0;
        for (int i = 0; i < d; i++) {
            double difference = r.get(i) - s.get(i);
            sum += difference * difference;
        }
        return Math.sqrt(sum);

    }

    /**
     * 给定{(distance, classification)}，会根据这个距离找出k个近邻
     * @param neighbors
     * @param k
     * @return
     */
    private static SortedMap<Double, String> findNearestK(Iterable<Tuple2<Double, String>> neighbors, int k) {
        //只保留k个邻近
        SortedMap<Double, String> nearestK = new TreeMap<>();
        for (Tuple2<Double, String> neighbor : neighbors) {
            Double distance = neighbor._1();
            String classificationID = neighbor._2();

            nearestK.put(distance, classificationID);
            if (nearestK.size() > k) {
                nearestK.remove(nearestK.lastKey());
            }

        }
        return nearestK;
    }

    /**
     * 统计分类的简单方法（根据多数计数选择分类）
     * @param nearestK
     * @return
     */
    private static Map<String, Integer> buildClassificationCount(Map<Double, String> nearestK) {
        Map<String, Integer> majority = new HashMap<String, Integer>();
        for (Map.Entry<Double, String> entry : nearestK.entrySet()) {
            String classificationID = entry.getValue();
            Integer count = majority.get(classificationID);
            if (count == null) {
                majority.put(classificationID, 1);
            } else {
                majority.put(classificationID, count + 1);
            }
        }
        return majority;
    }

    /**
     * 根据多数原则选择分类， 对一个给定的查询点r，如果k = 6，则分类为{C1， C2， C3， C4， C5， C6}
     * @param majority
     * @return
     */
    private static String classifyByMajority(Map<String, Integer> majority) {
        int votes = 0;
        String selectedClassification = null;
        for (Map.Entry<String, Integer> entry : majority.entrySet()) {
            if (selectedClassification == null) {
                selectedClassification = entry.getKey();
                votes = entry.getValue();
            } else {
                int count = entry.getValue();
                if (count > votes) {
                    selectedClassification = entry.getKey();
                    votes = count;
                }
            }
        }

        return selectedClassification;
    }

}

