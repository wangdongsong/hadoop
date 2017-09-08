package com.wds.dataalgorithms.kmeans;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.regex.Pattern;

/**
 * Created by wangdongsong1229@163.com on 2017/9/7.
 */
public class ParsePoint implements Function<String, Vector> {
    private static final Pattern SPACE = Pattern.compile(" ");

    @Override
    public Vector call(String v1) throws Exception {
        String[] tok = SPACE.split(v1);
        double[] point = new double[tok.length];
        for (int i = 0; i < tok.length; i++) {
            point[i] = Double.parseDouble(tok[i]);
        }
        return Vectors.dense(point);
    }
}
