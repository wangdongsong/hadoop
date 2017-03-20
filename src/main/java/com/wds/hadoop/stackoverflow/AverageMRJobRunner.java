package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * 平均值示例
 * 问题：给定用户的评论示例，按天计算每小时的平均评论长度
 * 方案：按小时分组，通过自定的Writable，将“计数”和“和“一直写到Writable类中，实现平均值，此方法可以使用combiner
 * Created by wangdongsong1229@163.com on 2017/3/18.
 */
public class AverageMRJobRunner {


    public static class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {
        private IntWritable outHour = new IntWritable();
        private CountAverageTuple outCountAverage = new CountAverageTuple();
        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            String strDate = parsed.get("CreationDate");
            String text = parsed.get("Text");

            try {
                Date creationDate = frmt.parse(strDate);
                outHour.set(creationDate.getHours());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            outCountAverage.setCount(1);
            outCountAverage.setAverage(text.length());

            context.write(outHour, outCountAverage);
        }
    }

    public static class AverageReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
        private CountAverageTuple result = new CountAverageTuple();

        @Override
        protected void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;

            for (CountAverageTuple val : values) {
                sum += val.getCount() * val.getAverage();
                count += val.getCount();
            }

            result.setCount(count);
            result.setAverage(sum / count);
            context.write(key, result);
        }
    }


    private static class CountAverageTuple implements Writable{
        private float count;
        private float average;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeFloat(count);
            out.writeFloat(average);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readLong();
            average = in.readLong();
        }

        public float getCount() {
            return count;
        }

        public void setCount(float count) {
            this.count = count;
        }

        public float getAverage() {
            return average;
        }

        public void setAverage(float average) {
            this.average = average;
        }
    }
}
