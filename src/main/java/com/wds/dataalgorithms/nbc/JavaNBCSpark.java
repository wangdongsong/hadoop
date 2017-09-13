package com.wds.dataalgorithms.nbc;

import com.wds.dataalgorithms.util.SparkUtil;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 贝叶斯 阶段1 使用符号训练数据建立分类器
 * Created by wangdongsong1229@163.com on 2017/9/10.
 */
public class JavaNBCSpark {

    public static void main(String[] args) throws Exception {
        //Step2 处理输入参数
        if (args.length < 1) {
            System.err.println("Usage:JavaNBCSpark <training-data-filename>");
            System.exit(1);
        }
        final String trainingDataFilename = args[0];


        //Step3 创建一个Spark上下文对象
        JavaSparkContext ctx = SparkUtil.createJavaSparkContext("java nbc");

        //Step4 读取训练数据
        JavaRDD<String> training = ctx.textFile(trainingDataFilename, 1);
        training.saveAsTextFile("/output/javaNBCSpark/output1");
        long trainingDataSize = training.count();

        //Step5 对训练数据的所有元素实现map函数
        JavaPairRDD<Tuple2<String, String>, Integer> pairs = training.flatMapToPair((rec) ->{
            String[] tokens = rec.split(",");
            int classificationindex = tokens.length - 1;
            final String theClassification = tokens[classificationindex];
            int end = classificationindex - 1;
            List<Tuple2<Tuple2<String, String>, Integer>> result = IntStream.range(0, end).mapToObj((i) -> {return new Tuple2<String, String>("CLASS", theClassification);}).map((k) -> {return new Tuple2<Tuple2<String, String>, Integer>(k, 1);}).collect(Collectors.toList());
            result.add(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>("CLASS", theClassification), 1));

            return result.iterator();
        });
        pairs.saveAsTextFile("/output/javaNBCSpark/output/2");

        //Step6 对训练数据的所有元素实现reduce函数
        JavaPairRDD<Tuple2<String, String>, Integer> counts = pairs.reduceByKey((i1, i2) -> {
            return i1 + i2;
        });
        counts.saveAsTextFile("/output/javaNBCSpark/output/3");

        //Step7  收集归约数据为Map
        Map<Tuple2<String, String>, Integer> countsAsMap = counts.collectAsMap();

        //Step8 建立分类器
        Map<Tuple2<String, String>, Double> PT = new HashMap<Tuple2<String, String>, Double>();
        List<String> CLASSIFICATIONS = new ArrayList<String>();
        for (Map.Entry<Tuple2<String, String>, Integer> entry : countsAsMap.entrySet()) {
            Tuple2<String, String> k = entry.getKey();
            String classification = k._2();
            if (k._1().equals("CLASS")) {
                PT.put(k, ((double) entry.getValue()) / ((double) trainingDataSize));
                CLASSIFICATIONS.add(k._2());
            } else {
                Tuple2<String, String> k2 = new Tuple2<>("CLASS", classification);
                Integer count = countsAsMap.get(k2);
                if (count == null) {
                    PT.put(k, 0.0);
                } else {
                    PT.put(k, ((double) entry.getValue()) / ((double) count.intValue()));
                }
            }
        }
        System.out.println("PT=" + PT);

        //Step9 保存分类器
        List<Tuple2<PairOfStrings, DoubleWritable>> list = toWritableList(PT);
        JavaPairRDD<PairOfStrings, DoubleWritable> ptRDD = ctx.parallelizePairs(list);
        ptRDD.saveAsHadoopFile("/output/javaNBCSpark/output/naivebayes/pt", PairOfStrings.class, DoubleWritable.class, SequenceFileOutputFormat.class);

        //Step9.1 用于对新记录分类的PT（概率表）

        //Step9.2 分类列表

    }

    private static List<Tuple2<PairOfStrings, DoubleWritable>> toWritableList(Map<Tuple2<String, String>, Double> PT){
        return  PT.entrySet().stream().map((entry) ->{
            return  new Tuple2<PairOfStrings, DoubleWritable>(new PairOfStrings(entry.getKey()._1(), entry.getKey()._2()), new DoubleWritable(entry.getValue()));
        }).collect(Collectors.toList());
        //return null;
    }


}
