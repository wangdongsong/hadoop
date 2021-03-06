package com.wds.hadoop.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.util.StringTokenizer;

/**
 * Created by wangdongsong1229@163.com on 2016/12/17.
 */
public class WordCountUsingToolRunner extends Configured implements Tool {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer str = new StringTokenizer(value.toString());
            while (str.hasMoreTokens()) {
                context.write(new Text(str.nextToken()), one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (org.apache.commons.lang.StringUtils.isAlphanumeric(key.toString()) && !StringUtils.isAlphaSpace(key.toString())) {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }

                context.write(key, new IntWritable(sum));
            }
        }
    }



    @Override
    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WordCount.WordCountMapper.class);
        job.setReducerClass(WordCount.WordCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new WordCountUsingToolRunner(), args);
    }
}
