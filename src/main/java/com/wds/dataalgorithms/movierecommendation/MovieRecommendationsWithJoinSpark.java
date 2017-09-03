package com.wds.dataalgorithms.movierecommendation;

import com.oracle.xmlns.internal.webservices.jaxws_databinding.JavaMethod;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * WithJoin
 *
 * Created by wangdongsong1229@163.com on 2017/9/2.
 */
public class MovieRecommendationsWithJoinSpark {

    public static void main(String[] args) {
        //Step2 处理输入参数
        if (args.length < 1) {
            System.err.println("Usage:MovieRecommendationsWithJoinSpark <users-ratings>");
            System.exit(0);
        }
        String usersRatingsInputFile = args[0];
        System.out.println("usersRatingsInputFile=" + usersRatingsInputFile);

        //Step3 创建上下文
        JavaSparkContext ctx = new JavaSparkContext();

        //Step4 读取输入文件并创建RDD
        JavaRDD<String> usersRatins = ctx.textFile(usersRatingsInputFile, 1);

        //Step5 找出谁曾对这个电影评分
        //Step5 - Step7 会找出每个电影的评分人数
        JavaPairRDD<String, Tuple2<String, Integer>> moviesRDD = usersRatins.mapToPair((s) ->{
            String[] record = s.split("\t");
            String user = record[0];
            String movie = record[1];
            Integer rating = new Integer(record[2]);
            Tuple2<String, Integer> userAndRating = new Tuple2<>(user, rating);
            return new Tuple2<>(movie, userAndRating);
        });
        //Debug
        moviesRDD.collect().forEach((t2) -> {
            System.out.println(t2._1() + "\t" + t2._2());
        });

        //Step6 按movie对movieRDD分组
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> moviesGrouped = moviesRDD.groupByKey();
        moviesGrouped.collect().forEach((t2) -> {
            System.out.println(t2._1() + "\t" + t2._2());
        });

        //Step7 找出每个电影的评分人数
        JavaPairRDD<String, Tuple3<String, Integer, Integer>> usersRDD = moviesGrouped.flatMapToPair((s) ->{
            List<Tuple2<String, Integer>> listOfUsersAndRatings = new ArrayList<>();
            String movie = s._1();
            Iterable<Tuple2<String, Integer>> pairsOfUserAndRating = s._2();
            int numberOfRatings = 0;

            for (Tuple2<String, Integer> t2 : pairsOfUserAndRating) {
                numberOfRatings++;
                listOfUsersAndRatings.add(t2);
            }


            //int finalNumberOfRatings = numberOfRatings;
            //TODO
//            StreamSupport.stream(listOfUsersAndRatings.spliterator(), false)
//                    .map((map) -> new Tuple3<String, Integer, Integer>(movie, map._2(), finalNumberOfRatings))
//                    .map((map2) -> new Tuple2())
            List<Tuple2<String, Tuple3<String, Integer, Integer>>> results = new ArrayList<>();
            for (Tuple2<String, Integer> t2 : listOfUsersAndRatings) {
                Tuple3 t3 = new Tuple3(movie, t2._2(), numberOfRatings);
                results.add(new Tuple2<String, Tuple3<String, Integer, Integer>>(t2._1(), t3));
            }
            return results.iterator();
        });

        //Step8 完成自连接
        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> joinedRDD = usersRDD.join(usersRDD);

        //Step9 删除重复的(movie1, movie2)对
        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> filteredRDD = joinedRDD.filter((s) -> {
            Tuple3<String, Integer, Integer> movie1 = s._2()._1();
            Tuple3<String, Integer, Integer> movie2 = s._2()._2();
            String movieName1 = movie1._1();
            String movieName2 = movie2._1();
            if (movieName1.compareTo(movieName2) < 0) {
                return true;
            } else {
                return false;
            }
        });

        //Step10 生成所有(movie1, movie2)组合
        JavaPairRDD<Tuple2<String, String>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> moviePairs = filteredRDD.mapToPair((s) ->{
            Tuple3<String, Integer, Integer> movie1 = s._2()._1();
            Tuple3<String, Integer, Integer> movie2 = s._2()._2();

            Tuple2<String, String> m1m2Key = new Tuple2<String, String>(movie1._1(), movie2._1());
            int ratingProduct = movie1._2() * movie2._2();

            int rating1Squared = movie1._2() * movie1._2();

            int rating2Squared = movie2._2() * movie2._2();

            Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> t7 = new Tuple7<>(movie1._2(), movie1._3(), movie2._2(), movie2._3(), ratingProduct, rating1Squared, rating2Squared);

            return new Tuple2<>(m1m2Key, t7);
        });

    }

}
