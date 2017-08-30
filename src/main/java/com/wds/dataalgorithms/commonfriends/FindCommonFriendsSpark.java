package com.wds.dataalgorithms.commonfriends;

import com.google.common.collect.Sets;
import com.google.gson.internal.Streams;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        JavaSparkContext ctx = new JavaSparkContext();

        //Step4 从HDFS读入文本文件
        JavaRDD<String> records = ctx.textFile(args[0], 1);

        //可使用下面代码调试Step4
        List<String> debug0 = records.collect();
        debug0.forEach(System.out::println);

        //Step5 将JavaRDD<String>映射到键-值对
        //应用映射器
        //其中Key=Tuple<user1, user2>， value=好友列表
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> pairs = records.flatMapToPair((s) ->{
            String[] tokens = s.split(",");
            long person = Long.parseLong(tokens[0]);
            String friendsAsString = tokens[1];
            String[] friendsTokenized = friendsAsString.split(" ");

            if (friendsTokenized.length == 1) {
                Tuple2<Long, Long> key = buildSortedTuple(person, Long.parseLong(friendsTokenized[0]));
            }

            List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> result = new ArrayList<>();

            //List<Long> friends = Stream.of(friendsTokenized).mapToLong(Long::parseLong).collect(Collectors.toList());
            List<Long> friends = Stream.of(friendsTokenized).map(Long::parseLong).collect(Collectors.toList());

            friends.forEach((f) ->{
                Tuple2<Long, Long> key = buildSortedTuple(person, f);
                result.add(new Tuple2<Tuple2<Long, Long>, Iterable<Long>>(key, friends));
            });
            return result.iterator();
        });
        //Debug step5
        pairs.collect().forEach((t2) -> {
            System.out.println("debug5 key=" + t2._1() + " \t value=" + t2._2());
        });

        //Step6 将(key=Tuple<u1, u2>, value=List<friends>对归约为(key=Tuple<u1, u2>, value=List<<ListFriends>>)
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Iterable<Long>>> grouped = pairs.groupByKey();
        grouped.collect().forEach((t2) -> {
            System.out.println("debug5 key=" + t2._1() + " \t value=" + t2._2());
        });

        //Step7 利用所有List<List<Long>>的交集查找共同好友
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> commonFriends = grouped.mapValues((s) ->{
            int size = 0;
            Map<Long, Integer> countCommon = new HashMap<>();

            for (Iterable<Long> iter : s) {
                size++;
                //TODO
                List<Long> list = StreamSupport.stream(iter.spliterator(), false).collect(Collectors.toList());
                if ((list == null) || (list.isEmpty())) {
                    continue;
                }

                for (Long f : list) {
                    Integer count = countCommon.get(f);
                    if (count == null) {
                        countCommon.put(f, 1);
                    } else {
                        countCommon.put(f, ++count);
                    }
                }
            }

            List<Long> finalCommonFriends = new ArrayList<>();
            for (Map.Entry<Long, Integer> entry : countCommon.entrySet()) {
                if (entry.getValue() == size) {
                    finalCommonFriends.add(entry.getKey());
                }
            }

            return finalCommonFriends;
        });


        //通过reduceByKey()转换器合并Step6和7
        //输入pairs(K: Tuple2<Long, Long>, V: Iterable<Long>)
        //输出commonfriends (K: String, V: Iterable<Long>)
        //转换器 reduceByKey()
        commonFriends = pairs.reduceByKey((a, b) ->{
            final Set<Long> x = Sets.newHashSet(a);
            return Sets.newHashSet(b).stream().filter((x::contains)).collect(Collectors.toSet());
        });

    }


    private static Tuple2<Long, Long> buildSortedTuple(long a, long b) {
        if (a < b) {
            return new Tuple2<Long, Long>(a, b);
        } else {
            return new Tuple2<Long, Long>(b, a);
        }
    }

}
