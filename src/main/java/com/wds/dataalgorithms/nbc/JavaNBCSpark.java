package com.wds.dataalgorithms.nbc;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DoubleWritable;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 贝叶斯
 * Created by wangdongsong1229@163.com on 2017/9/10.
 */
public class JavaNBCSpark {

    public static void main(String[] args) {

    }

    private static List<Tuple2<PairOfStrings, DoubleWritable>> toWritableList(Map<Tuple2<String, String>, Double> PT){
        return  PT.entrySet().stream().map((entry) ->{
            return  new Tuple2<PairOfStrings, DoubleWritable>(new PairOfStrings(entry.getKey()._1(), entry.getKey()._2()), new DoubleWritable(entry.getValue()));
        }).collect(Collectors.toList());
        //return null;
    }


}
