package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.DoubleAccumulator;

/**
 * 分布式Grep过滤示例
 * Created by wangdongsong1229@163.com on 2017/3/22.
 */
public class GrepFilterMRJobRunner extends Configured implements Tool{

    @Override
    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        if (args != null && args.length < 3) {
            System.err.println("Usage: GrepFilterMRJobRunner <in> <out><srs>");
            return 0;
        }
        Job job = Job.getInstance(getConf());
        job.setJarByClass(GrepFilterMRJobRunner.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        if ("srs".equalsIgnoreCase(args[2])) {
            job.setMapperClass(SRSMaper.class);
        } else {
            job.setMapperClass(GrepMapper.class);
        }

        job.setNumReduceTasks(0); // Set number of reducers to zero
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.submit();
       return  job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new GrepFilterMRJobRunner(), args);
    }

    /**
     * Grep Mapper
     */
    public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {
        private String mapRegex = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mapRegex = context.getConfiguration().get("mapregex");
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().matches(mapRegex)) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    /**
     * 简单随机抽样（Simple Random Sampling, SRS）
     */
    public static class SRSMaper extends Mapper<Object, Text, NullWritable, Text> {
        private Random rands = new Random();
        private Double percentage;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String stePercentage = context.getConfiguration().get("filter_percentage");
            percentage = Double.parseDouble(stePercentage) / 100.0;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (rands.nextDouble() < percentage) {
                context.write(NullWritable.get(), value);
            }
        }
    }


}
