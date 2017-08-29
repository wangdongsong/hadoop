package com.wds.dataalgorithms.commonfriends;

/**
 * 查找共同好友 Spark解决方案
 * Created by wangdongsong1229@163.com on 2017/8/29.
 */
public class FindCommonFriendsSpark {

    public static void main(String[] args) {
        //Step2 检查输入参数
        if (args.length < 1) {
            System.err.println("Usage: FindCommonFriendsSpark <file>");
            System.exit(0);
        }
        System.out.println("HDFS input file = " + args[0]);

        //Step3 创建一个JavaSparkContext对象
        //Step4 从HDFS读入文本文件
        //创建第一个JavaRDD表示输入文件
        //Step5 将JavaRDD<String>映射到键-值对
        //其中Key=Tuple<user1, user2>， value=好友列表
        //Step6 将(key=Tuple<u1, u2>, value=List<friends>对归约为(key=Tuple<u1, u2>, value=List<<ListFriends>>)
        //Step7 利用所有List<List<Long>>的交集查找共同好友

    }

}
