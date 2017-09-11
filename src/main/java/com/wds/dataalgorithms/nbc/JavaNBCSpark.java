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
        //Step2 处理输入参数

        //Step3 创建一个Spark上下文对象

        //Step4 读取训练数据

        //Step5 对训练数据的所有元素实现map函数

        //Step6 对训练数据的所有元素实现reduce函数

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
