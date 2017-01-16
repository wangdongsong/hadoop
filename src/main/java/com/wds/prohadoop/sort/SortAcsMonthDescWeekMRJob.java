package com.wds.prohadoop.sort;

import com.wds.prohadoop.utils.AirlineDataUtils;
import com.wds.prohadoop.utils.DelaysWritable;
import com.wds.prohadoop.utils.MonthDoWWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by wangdongsong1229@163.com on 2017/1/14.
 */
public class SortAcsMonthDescWeekMRJob extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortAcsMonthDescWeekMRJob.class);

        job.setMapOutputKeyClass(MonthDoWWritable.class);
        job.setMapOutputValueClass(DelaysWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Use ComparatorForMonthDoWOnlyWritable
        //job.setSortComparatorClass(ComparatorForMonthDoWOnlyWritable.class);

        String[] ags = new GenericOptionsParser(conf, args).getRemainingArgs();

        FileInputFormat.setInputPaths(job, new Path(ags[0]));
        FileOutputFormat.setOutputPath(job, new Path(ags[1]));

        job.submit();

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SortAcsMonthDescWeekMRJob(), args);
    }

    public static class SortAcsMonthDescWeekMapper extends Mapper<LongWritable, Text, MonthDoWWritable, DelaysWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                String[] contents = value.toString().split(",");
                String month = AirlineDataUtils.getMonth(contents);
                String dow = AirlineDataUtils.getDayOfTheWeek(contents);
                MonthDoWWritable mw = new MonthDoWWritable();
                mw.month = new IntWritable(Integer.parseInt(month));
                mw.dayOfWeek = new IntWritable(Integer.parseInt(dow));

                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(value.toString());

                context.write(mw, dw);
            }
        }
    }

    public static class SortAscMonthDescWeekReducer extends Reducer<MonthDoWWritable, DelaysWritable, NullWritable, Text> {
        @Override
        protected void reduce(MonthDoWWritable key, Iterable<DelaysWritable> values, Context context) throws IOException, InterruptedException {
            for (DelaysWritable val : values) {
                Text t = AirlineDataUtils.parseDelaysWritableToText(val);
                context.write(NullWritable.get(), t);
            }
        }
    }

}
