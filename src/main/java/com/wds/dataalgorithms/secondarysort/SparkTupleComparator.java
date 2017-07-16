package com.wds.dataalgorithms.secondarysort;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by wangdongsong1229@163.com on 2017/7/16.
 */
public class SparkTupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {

    public static final SparkTupleComparator INSTANCE = new SparkTupleComparator();

    private SparkTupleComparator(){}

    @Override
    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
        return o1._1.compareTo(o2._1);
    }
}
