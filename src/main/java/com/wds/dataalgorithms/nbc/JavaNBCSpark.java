package com.wds.dataalgorithms.nbc;

import com.wds.dataalgorithms.util.SparkUtil;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 贝叶斯
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

        //Step8 建立分类器

        //Step9 保存分类器

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
