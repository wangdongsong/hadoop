package com.wds.dataalgorithms.kmeans;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Created by wangdongsong1229@163.com on 2017/9/7.
 */
public class JavaKMeansSpark {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: JavaKMeansSpark <input file> <k> <max iterations> [<runs>]");
            System.exit(0);
        }
        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);
        int runs = 1;
        if (args.length >= 4) {
            runs = Integer.parseInt(args[3]);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaKMeansSpark");
        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> lines = ctx.textFile(inputFile);
        JavaRDD<Vector> points = lines.map(new ParsePoint());
        KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs, KMeans.K_MEANS_PARALLEL());
        System.out.println("Cluster centers:");
        for (Vector vector : model.clusterCenters()) {
            System.out.println(" " + vector);
        }
        double cost = model.computeCost(points.rdd());
        System.out.println("Cost:" + cost);
        ctx.stop();
    }

}
