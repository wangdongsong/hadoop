package com.wds.prohadoop.select;

import com.wds.prohadoop.utils.AirlineDataUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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
 * Partitioner
 * Created by wangdongsong1229@163.com on 2017/1/10.
 */
public class SplitByMonthMRJob extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new SplitByMonthMRJob(), args);
    }

    @Override
    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(SplitByMonthMRJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SplitByMonthMapper.class);
        job.setReducerClass(SplitByMonthReducer.class);
        job.setPartitionerClass(MonthPartitioner.class);
        job.setNumReduceTasks(12);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);

        return status ? 0: 1;
    }

    public static final class SplitByMonthMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                int month = Integer.parseInt(AirlineDataUtils.getMonth(value.toString().split(",")));
                context.write(new IntWritable(month), value);
            }
        }
    }

    public static class SplitByMonthReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text output : values) {
                context.write(NullWritable.get(), new Text(output));
            }
        }
    }

    public static class MonthPartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable intWritable, Text text, int numPartitions) {
            return (intWritable.get() - 1);
        }
    }




}
