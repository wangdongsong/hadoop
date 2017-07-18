package com.wds.dataalgorithms.topN;

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

}
