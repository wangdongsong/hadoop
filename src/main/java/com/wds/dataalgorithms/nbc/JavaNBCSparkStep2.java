package com.wds.dataalgorithms.nbc;

import com.wds.dataalgorithms.util.SparkUtil;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.avro.mapred.SequenceFileInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * 朴素贝叶斯分类器，阶段2：使用阶段创建分类器对新数据进行分类
 * Created by wangdongsong1229@163.com on 2017/9/13.
 */
public class JavaNBCSparkStep2 {

    public static void main(String[] args) throws Exception {

        //Step2 处理输入参数
        if (args.length != 2) {
            System.err.println("Usage:JavaNBCSparkStep2 <input-data-filename> <NP-PT-path>");
            System.exit(0);
        }
        final String inputDataFilename = args[0];
        final String nbProbabilityTablePath = args[1];

        //Step3 创建Spark上下文
        JavaSparkContext ctx = SparkUtil.createJavaSparkContext("JavaNBCSparkStep2");

        //Step4 读取要分类的新数据
        JavaRDD<String> newdata = ctx.textFile(inputDataFilename, 1);

        //Step5 从Hadoop读取分类器
        JavaPairRDD<PairOfStrings, DoubleWritable> ptRDD = ctx.hadoopFile(nbProbabilityTablePath, SequenceFileInputFormat.class, PairOfStrings.class, DoubleWritable.class);
        JavaPairRDD<Tuple2<String, String>, Double> classifierRDD = ptRDD.mapToPair((Tuple2<PairOfStrings, DoubleWritable> rec) -> {
            PairOfStrings pair = rec._1();
            Tuple2<String, String> k2 = new Tuple2<String, String>(pair.getLeftElement(), pair.getRightElement());
            Double v2 = new Double(rec._2().get());
            return new Tuple2<>(k2, v2);
        });

        //Step6 缓存分类器组件
        //集群中的任何节点都可以使用这些组件
        Map<Tuple2<String, String>, Double> classfier = classifierRDD.collectAsMap();
        final Broadcast<Map<Tuple2<String, String>, Double>> broadcastClassifier = ctx.broadcast(classfier);

        JavaRDD<String> classesRDD = ctx.textFile("/output/javaNBCSparkStep2/output/naivebayes/classes", 1);
        List<String> CLASSES = classesRDD.collect();
        final Broadcast<List<String>> broadcastClasses = ctx.broadcast(CLASSES);

        //Step7 对新数据分类
        JavaPairRDD<String, String> classified = newdata.mapToPair((String rec) -> {
            Map<Tuple2<String, String>, Double> CLASSIFIER = broadcastClassifier.value();
            List<String> classes = broadcastClasses.value();

            String[] attributes = rec.split(",");
            String selectedClass = null;
            double maxPosterior = 0.0;

            for (String aClass : classes) {
                double posterior = CLASSIFIER.get(new Tuple2<String, String>("CLASS", aClass));
                for (int i = 0; i < attributes.length; i++) {
                    Double probability = CLASSIFIER.get(new Tuple2<String, String>(attributes[i], aClass));
                    if (probability == null) {
                        posterior = 0.0;
                        break;
                    } else {
                        posterior *= probability.doubleValue();
                    }
                }
                if (selectedClass == null) {
                    selectedClass = aClass;
                    maxPosterior = posterior;
                } else {
                    if (posterior > maxPosterior) {
                        selectedClass = aClass;
                        maxPosterior = posterior;
                    }
                }
            }
            return new Tuple2<String, String>(rec, selectedClass);
        });

        classified.saveAsTextFile("/output/javaNBCSparkStep2/output/classified");
    }

}
