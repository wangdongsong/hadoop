package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * 并行作业链
 *
 * 问题：给定前例输出的已分好箱的用户，并行执行作业计算每个箱中用户的平均声望
 * Created by wangdongsong1229@163.com on 2017/3/26.
 */
public class ParallelJobChainMRJobRunner extends Configured implements Tool{

    @Override
    public int run(String[] allArgs) throws Exception {
        Configuration conf = new Configuration();
        String[] args = new GenericOptionsParser(conf, allArgs)
                .getRemainingArgs();

        if (args.length != 4) {
            System.err
                    .println("Usage: ParallelJobs <below-avg-in> <below-avg-out> <below-avg-out> <above-avg-out>");
            System.exit(2);
        }

        Path belowAvgInputDir = new Path(args[0]);
        Path aboveAvgInputDir = new Path(args[1]);
        Path belowAvgOutputDir = new Path(args[2]);
        Path aboveAvgOutputDir = new Path(args[3]);

        Job belowAvgJob = submitJob(conf, belowAvgInputDir, belowAvgOutputDir);
        Job aboveAvgJob = submitJob(conf, aboveAvgInputDir, aboveAvgOutputDir);

        while (!belowAvgJob.isComplete() || !aboveAvgJob.isComplete()) {
            Thread.sleep(500);
        }

        if (belowAvgJob.isSuccessful()) {
            System.out.println("Below average job completed successfully");
        } else {
            System.out.println("Below average job failed");
        }

        if (aboveAvgJob.isSuccessful()) {
            System.out.println("Below average job completed successfully");
        } else {
            System.out.println("Below average job failed");
        }


        return belowAvgJob.isSuccessful() && aboveAvgJob.isSuccessful() ? 0 : 1;
    }

    private Job submitJob(Configuration conf, Path inputDir, Path outputDir) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "ParallelJobChainMRJobRunner");
        job.setJarByClass(ParallelJobChainMRJobRunner.class);

        job.setMapperClass(AverageReputationMapper.class);
        job.setReducerClass(AverageReputationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, inputDir);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);

        job.submit();
        return job;
    }

    public static void main(String[] args) {

    }

    public static class AverageReputationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private static final Text GROUP_ALL_KEY = new Text("Average Reputation");
        private DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            double reputaion = Double.parseDouble(tokens[2]);
            outValue.set(reputaion);
            context.write(GROUP_ALL_KEY, outValue);
        }
    }

    public static class AverageReputationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double count = 0;

            for (DoubleWritable dw : values) {
                sum += dw.get();
                ++count;
            }

            outValue.set(sum / count);
            context.write(key, outValue);
        }
    }




}
