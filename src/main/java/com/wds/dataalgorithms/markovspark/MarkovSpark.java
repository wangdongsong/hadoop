package com.wds.dataalgorithms.markovspark;

import com.wds.dataalgorithms.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by wangdongsong1229@163.com on 2017/9/4.
 */
public class MarkovSpark implements Serializable {

    public static void main(String[] args) {
        //Step1 处理输入参数
        if (args.length != 1) {
            System.err.println("Usage: MarkovSpark <input-path>");
            System.exit(0);
        }

        final String inputPath = args[0];
        System.out.println("inputPath:args[0]=" + args[0]);

        //Step2 创建上下文，并把输入转换为RDD
        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> records = ctx.textFile(inputPath, 1);

        //Step3 RDD转换为JavaPairRDD
        JavaPairRDD<String, Tuple2<Long, Integer>> kv = records.mapToPair((rec) ->{
            String[] tokens = StringUtils.split(rec, ",");
            if (tokens.length != 4) {
                return null;
            }
            long date = 0;
            date = DateUtil.getDateAsMilliSeconds(tokens[2]);
            int amout = Integer.parseInt(tokens[3]);
            Tuple2<Long, Integer> v = new Tuple2<>(date, amout);
            return new Tuple2<>(tokens[0], v);
        });
        kv.saveAsTextFile("/output/markov/output/3");

        //Step4 按CustomerID对交易分组
        JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> customerRDD = kv.groupByKey();
        customerRDD.saveAsTextFile("/output/markov/output/4");

        //Step5 创建马尔可夫状态序列
        JavaPairRDD<String, List<String>> stateSequence = customerRDD.mapValues((Iterable<Tuple2<Long, Integer>> dateAndAmout) -> {
            List<Tuple2<Long, Integer>> list = toList(dateAndAmout);
            Collections.sort(list, TupleComparatorAscending.INSTANCE);
            return toStateSequence(list);
        });

        //Step6 生成马尔可夫状态转移矩阵
        JavaPairRDD<Tuple2<String, String>, Integer> model1 = stateSequence.flatMapToPair(stringListTuple2 -> {
            List<String> states = stringListTuple2._2();
            if ((states == null) || (states.size() < 2)) {
                return Collections.emptyIterator();
            }

            List<Tuple2<Tuple2<String, String>, Integer>> mapperOutput = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
            for (int i = 0; i < (states.size() -1); i++) {
                String fromState = states.get(i);
                String toState = states.get(i + 1);
                Tuple2<String, String> k = new Tuple2<>(fromState, toState);
                mapperOutput.add(new Tuple2<Tuple2<String, String>, Integer>(k, 1));
            }

            return mapperOutput.iterator();
        });

        //组合/归约
        JavaPairRDD<Tuple2<String, String>, Integer> markovModel1 = model1.reduceByKey((i1, i2) -> i1 + i2);

        //Step7 最终输出
        JavaRDD<String> markovModelFormatted = markovModel1.map((t) -> {
            return t._1()._1() + ", " + t._1()._2() + ", " + t._2();
        });

    }

    private static List<String> toStateSequence(List<Tuple2<Long, Integer>> list) {
        if (list.size() < 2) {
            return null;
        }

        List<String> stateSequence = new ArrayList<String>();
        Tuple2<Long, Integer> prior = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            Tuple2<Long,Integer> current = list.get(i);
            //
            long priorDate = prior._1;
            long date = current._1;
            // one day = 24*60*60*1000 = 86400000 milliseconds
            long daysDiff = (date - priorDate) / 86400000;

            int priorAmount = prior._2;
            int amount = current._2;
            //
            String elapsedTime = getElapsedTime(daysDiff);
            String amountRange = getAmountRange(priorAmount, amount);
            //
            String element = elapsedTime + amountRange;
            stateSequence.add(element);
            prior = current;
        }
        return stateSequence;
    }

    private static List<Tuple2<Long, Integer>> toList(Iterable<Tuple2<Long, Integer>> dateAndAmout) {
        return StreamSupport.stream(dateAndAmout.spliterator(), false).collect(Collectors.toList());
    }

    static class TupleComparatorAscending implements Comparator<Tuple2<Long, Integer>>, Serializable {
        final static TupleComparatorAscending INSTANCE = new TupleComparatorAscending();
        @Override
        public int compare(Tuple2<Long, Integer> t1, Tuple2<Long, Integer> t2) {
            // return -t1._1.compareTo(t2._1);     // sorts RDD elements descending
            return t1._1.compareTo(t2._1);         // sorts RDD elements ascending
        }
    }

    static String getElapsedTime(long daysDiff) {
        if (daysDiff < 30) {
            return "S"; // small
        } else if (daysDiff < 60) {
            return "M"; // medium
        } else {
            return "L"; // large
        }
    }

    static String getAmountRange(int priorAmount, int amount) {
        if (priorAmount < 0.9 * amount) {
            return "L"; // significantly less than
        } else if (priorAmount < 1.1 * amount) {
            return "E"; // more or less same
        } else {
            return "G"; // significantly greater than
        }
    }

}
