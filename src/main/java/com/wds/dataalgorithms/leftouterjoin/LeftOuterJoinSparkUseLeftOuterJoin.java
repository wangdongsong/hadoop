package com.wds.dataalgorithms.leftouterjoin;

/**
 * 使用LeftOuterJoin的Spark实现，MapReduce没有提供类似的方法
 *
 * 使用LeftOuterJoin方法可以避免以下问题：
 *
 * 1、对users和transactions使用JavaPairRDD.union操作，开销大
 * 2、引入定制标志，如地址增加"L"，为商品增加“P”
 * 3、使用额外的RDD转换来区分定制标志
 *
 * Created by wangdongsong1229@163.com on 2017/7/27.
 */
public class LeftOuterJoinSparkUseLeftOuterJoin {

}
