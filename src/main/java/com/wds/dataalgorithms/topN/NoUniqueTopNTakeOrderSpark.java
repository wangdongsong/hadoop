package com.wds.dataalgorithms.topN;

/**
 * 使用takeOrdered的Spark Top 10 解决方案
 *
 * 假设：对于所有输入（K，V），K不是唯一的。
 * 这个类实现了Top N设计模式（N > 0）
 *
 * 对于输入（K，V）对，K非唯一，说明会看到类似（A，1）...（A，5）的输入数据
 *
 * 1、映射输入 =>（K，V）
 *
 * 2、归约（K，List<V1, V2, V3 ...Vn>） =>  (K, V），其中V=V1 + V2 + ... +Vn
 *
 * 3、使用takeOrdered找到Top N
 *
 * Created by wangdongsong1229@163.com on 2017/7/20.
 */
public class NoUniqueTopNTakeOrderSpark {
}
