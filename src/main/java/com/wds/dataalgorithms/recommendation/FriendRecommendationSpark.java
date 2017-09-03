package com.wds.dataalgorithms.recommendation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * 好友推荐
 * Created by wangdongsong1229@163.com on 2017/9/1.
 */
public class FriendRecommendationSpark {

    public static void main(String[] args) {
        //Step2 处理输入参数
        if (args.length < 1) {
            System.err.println("Usage: FriendRecommendationSpark <users and friends>");
            System.exit(0);
        }
        String hdfsInputFile = args[0];

        //Step3 创建Spark上下文
        JavaSparkContext ctx = new JavaSparkContext();

        //Step4 读文件并创建RDD
        JavaRDD<String> records = ctx.textFile(hdfsInputFile, 1);

        //可使用如下代码调试
        List<String> debug1 = records.collect();
        debug1.forEach(System.out::println);

        //Step5 实现map函数
        JavaPairRDD<Long, Tuple2<Long, Long>> pairs = records.flatMapToPair((record) ->{
            String[] tokens = record.split("\t");
            long person = Long.parseLong(tokens[0]);
            String friendsString = tokens[1];
            String[] friendsTokenized = friendsString.split(",");

            List<Long> friends = new ArrayList<>();
            List<Tuple2<Long, Tuple2<Long, Long>>> mapperOutput = new ArrayList<Tuple2<Long, Tuple2<Long, Long>>>();

            for (String friendAsString : friendsTokenized) {
                long toUser = Long.parseLong(friendAsString);
                friends.add(toUser);
                Tuple2<Long, Long> directFriend = T2(toUser, -1L);
                mapperOutput.add(T2(person, directFriend));
            }

            for (int i = 0; i < friends.size(); i++) {
                for (int j = i + 1; j < friends.size(); j++) {
                    Tuple2<Long, Long> possibleFriend1 = T2(friends.get(j), person);
                    mapperOutput.add(T2(friends.get(i), possibleFriend1));
                    Tuple2<Long, Long> possibleFriend2 = T2(friends.get(i), person);
                    mapperOutput.add(T2(friends.get(i), possibleFriend2));
                }
            }

            return mapperOutput.iterator();
        });
        //Debug
        pairs.collect().forEach((t2) ->{
            System.out.println("debug2 key=" + t2._1() + "\t value=" + t2._2());
        });

        //Step6 实现reduce函数
        JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> grouped = pairs.groupByKey();

        //Step7 生成所需要的最终输出
        JavaPairRDD<Long, String> recommendations = grouped.mapValues((values) ->{
            //mutualFriends.key 推荐好友
            //mutualFriends.value 共同好友列表
            final Map<Long, List<Long>> mutualFriends = new HashMap<>();
            for (Tuple2<Long, Long> t2 : values) {
                final Long toUser = t2._1();
                final Long mutualFriend = t2._2();
                final boolean alreadyFriend = (mutualFriend == -1);

                if (mutualFriends.containsKey(toUser)) {
                    if (alreadyFriend) {
                        mutualFriends.put(toUser, null);
                    } else if (mutualFriends.get(toUser) != null) {
                        mutualFriends.get(toUser).add(mutualFriend);
                    }
                } else {
                    if (alreadyFriend) {
                        mutualFriends.put(toUser, null);
                    } else {
                        List<Long> list1 = new ArrayList<>(Arrays.asList(mutualFriend));
                        mutualFriends.put(toUser, list1);
                    }
                }
            }
            return buildRecommendations(mutualFriends);
        });

        ctx.close();
    }

    /**
     * 工具方法
     * @param mutualFriends
     * @return
     */
    private static String buildRecommendations(Map<Long, List<Long>> mutualFriends) {
        StringBuilder recommendations = new StringBuilder();
        mutualFriends.entrySet().stream().filter((entry) -> entry.getValue() != null).forEach((entry) ->{
            recommendations.append(entry.getKey());
            recommendations.append(" (");
            recommendations.append(entry.getValue().size());
            recommendations.append(":");
            recommendations.append(entry.getValue());
            recommendations.append("),");
        });
        return recommendations.toString();
    }

    private static Tuple2<Long, Long> T2(long a, long b) {
        return new Tuple2<>(a, b);
    }

    private static Tuple2<Long, Tuple2<Long, Long>> T2(long a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a, b);
    }

}
