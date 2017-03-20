package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.FloatSplitter;
import org.apache.hadoop.util.Tool;
import org.apache.http.impl.conn.Wire;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 问题：给定一个用户评论的列表，确定其一天中每个小时的评论长度的中位数和标准差
 * 方案1：不使用combiner，因不能简单的直接使用Reducer作为combiner
 * 方案2：优化版本的combiner
 *
 * Created by wangdongsong1229@163.com on 2017/3/20.
 */
public class MedianStdDevMRJobRunner extends Configured implements Tool{
    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable outHour = new IntWritable();
        private IntWritable outCommentLength = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String strDate = parsed.get("CreationDate");

            try {
                Date creationDate = frmt.parse(strDate);
                outHour.set(creationDate.getHours());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            outCommentLength.set(parsed.get("text").length());

            context.write(outHour, outCommentLength);
        }
    }


    /**
     * 在这个实现当中，不能使用combiner优化。Reducer需要用一个键对应的所有值来计算出中位数和标准差
     * 因为combiner只能在一个map的本地处理当前map的中间健/值对，所以无法在map端计算一个键的中位数和标准差
     */
    public static class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdTuple> {
        private MedianStdTuple result = new MedianStdTuple();
        private List<Float> commentLengths = new ArrayList<>();


        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;
            commentLengths.clear();
            result.setStdDev(0);

            //针对每一个key，遍历其值
            for (IntWritable val : values) {
                commentLengths.add((float) val.get());
                sum += val.get();
                ++count;
            }
            //排序用于计算中位数
            Collections.sort(commentLengths);

            //偶数时，中间的两个数的平均值
            if (count % 2 == 0) {
                result.setMedian((commentLengths.get((int) count / 2 - 1) + commentLengths.get((int) count / 2)) / 2.0f);
            } else {
                result.setMedian(commentLengths.get((int) count / 2));
            }

            //计算标准差
            float mean = sum / count;
            float sumOfSquares = 0.0f;
            for (Float f : commentLengths) {
                sumOfSquares += (f - mean) * (f - mean);
            }

            result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));
            context.write(key, result);
        }
    }

    /**
     * 优化的Mapper
     */
    public static class OptimzingMedianStdDevMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable> {
        private IntWritable commentLength = new IntWritable();
        private static final LongWritable ONE = new LongWritable();
        private IntWritable outHour = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String strDate = parsed.get("CreationDate");

            try {
                Date creationDate = frmt.parse(strDate);
                outHour.set(creationDate.getHours());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            commentLength.set(parsed.get("Text").length());
            SortedMapWritable outCommentLength = new SortedMapWritable();
            outCommentLength.put(commentLength, ONE);
            context.write(outHour, outCommentLength);
        }
    }

    public static class OptimzingMedianStdDevReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdTuple> {
        private MedianStdTuple result = new MedianStdTuple();
        private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<>();

        @Override
        protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            long totalComments = 0;
            commentLengthCounts.clear();
            result.setMedian(0);
            result.setStdDev(0);

            for (SortedMapWritable v : values) {
                for (Map.Entry<WritableComparable, Writable> entry : v.entrySet()) {
                    int length = ((IntWritable) entry.getKey()).get();
                    long count = ((LongWritable) entry.getKey()).get();
                    totalComments += count;
                    sum += length * count;

                    Long storedCount = commentLengthCounts.get(length);

                    if (storedCount == null) {
                        commentLengthCounts.put(length, count);
                    } else {
                        commentLengthCounts.put(length, storedCount + count);
                    }
                }
            }

            long medianIndex = totalComments / 2L;
            long previousComments = 0;
            long comments = 0;
            int prevKey = 0;

            for (Map.Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
                comments = previousComments + entry.getValue();

                if (previousComments <= medianIndex && medianIndex < comments) {
                    if (totalComments % 2 == 0 && previousComments == medianIndex) {
                        result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
                    } else {
                        result.setMedian(entry.getKey());
                    }
                    break;
                }

                previousComments = comments;
                prevKey = entry.getKey();
            }

            float mean = sum / totalComments;
            float sumOfSquares = 0.0f;
            for (Map.Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
                sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getKey();
            }

            result.setStdDev((float) Math.sqrt(sumOfSquares / (totalComments - 1)));
            context.write(key, result);
        }
    }



    private static class MedianStdTuple implements Writable{

        private float stdDev;
        private float median;

        @Override
        public void write(DataOutput out) throws IOException {

        }

        @Override
        public void readFields(DataInput in) throws IOException {

        }

        public void setStdDev(float stdDev) {
            this.stdDev = stdDev;
        }

        public void setMedian(float median) {
            this.median = median;
        }
    }
}
