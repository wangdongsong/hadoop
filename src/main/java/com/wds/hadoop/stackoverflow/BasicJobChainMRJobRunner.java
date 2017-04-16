package com.wds.hadoop.stackoverflow;

import com.google.gson.internal.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;


/**
 * 基本作业链
 *
 * 该示例的目标是输出一份用户列表，其中包含用户声望以及每个用户发表的帖子数，该目标可以通过一个单独的MapReduce作业实现
 * ，但又想将用户分成两部分，一部分是发帖数在平均值以上的，一部分是发帖数在平均值以下的。因此需要两个作业来执行计数，另一个
 * 作业根据用户的发帖数将用户分配到两个不同的箱中。
 *
 * 该示例会使用到4种模式：数值概要、计数、分箱、复制连接
 *
 * 问题：给定由StackOverflow帖子组成的一个数据集，每个用户按照发贴数是高于还是低于平均分帖数分类。当生成输出时，从一个独立
 * 的数据集中获得每个用户的声望以丰富用户信息
 *
 * Created by wangdongsong1229@163.com on 2017/3/26.
 */
public class BasicJobChainMRJobRunner extends Configured implements Tool {
    public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
    public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "aboveavg";
    public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "belowavg";

    @Override
    public int run(String[] allArgs) throws Exception {
        Configuration conf = new Configuration();
        String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();

        if (args.length != 3) {
            System.err.println("Usage: BasicJobChainMRJobRunner <posts> <users> <out>");
            System.exit(2);
        }

        Path postPath = new Path(args[0]);
        Path userPath = new Path(args[1]);
        Path outputPathInterMediate = new Path(args[2] + "_int");
        Path outputPath = new Path(args[2]);

        Job basicJob = Job.getInstance(conf, "BasicJobChainMRJobRunner");
        basicJob.setJarByClass(BasicJobChainMRJobRunner.class);
        basicJob.setMapperClass(UserIdMapper.class);
        basicJob.setCombinerClass(LongSumReducer.class);
        basicJob.setReducerClass(UserIdSumReducer.class);

        basicJob.setOutputKeyClass(Text.class);
        basicJob.setOutputValueClass(LongWritable.class);

        basicJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(basicJob, postPath);

        basicJob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(basicJob, outputPathInterMediate);

        int code = basicJob.waitForCompletion(true) ? 0 : 1;

        //执行第二个作业之前一定要先检查第一个作业是否成功
        if (code == 0) {
            double numRecords = (double) basicJob.getCounters().findCounter(AVERAGE_CALC_GROUP, UserIdMapper.RECORD_COUNTER_NAME).getValue();
            double numUsers = (double) basicJob.getCounters().findCounter(AVERAGE_CALC_GROUP, UserIdSumReducer.USERS_COUNTER_NAME).getValue();
            double averagePostsPerUser = numRecords / numUsers;

            Job binningJob = Job.getInstance(new Configuration(), "BasicJobChainMRJobRunner-Binning");
            binningJob.setJarByClass(BasicJobChainMRJobRunner.class);
            binningJob.setMapperClass(UserIdBinningMapper.class);
            UserIdBinningMapper.setAveragePostsPerUser(binningJob, averagePostsPerUser);

            binningJob.setNumReduceTasks(0);
            binningJob.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(binningJob, outputPathInterMediate);

            MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_ABOVE_NAME, TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_BELOW_NAME, TextOutputFormat.class, Text.class, Text.class);

            MultipleOutputs.setCountersEnabled(binningJob, true);
            TextOutputFormat.setOutputPath(binningJob, outputPath);

            Stream.of(FileSystem.get(conf).listStatus(userPath)).forEach(path -> basicJob.addCacheFile(path.getPath().toUri()));
            code = binningJob.waitForCompletion(true) ? 0 : 1;

        }
        FileSystem.get(conf).delete(outputPathInterMediate, true);

        return code;
    }

    public static class UserIdMapper extends Mapper<Object, Text, Text, LongWritable> {
        public static final String RECORD_COUNTER_NAME = "Records";
        private static final LongWritable ONE = new LongWritable(1);
        private Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String userId = parsed.get("OwnerUserId");
            if (userId != null) {
                outKey.set(userId);
                context.write(outKey, ONE);
                context.getCounter(AVERAGE_CALC_GROUP, RECORD_COUNTER_NAME).increment(1);
            }
        }
    }

    public static class UserIdSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public static final String USERS_COUNTER_NAME = "Users";
        private LongWritable outValue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.getCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME).increment(1);
            final LongAdder sum = new LongAdder();
            values.forEach(v -> sum.add(v.get()));
            //Lambda的替代方式
            //for (LongWritable v : values) {
            //    sum += v.get();
            //}
            outValue.set(sum.longValue());
            context.write(key, outValue);
        }
    }

    public static class UserIdBinningMapper extends Mapper<Object, Text, Text, Text> {
        public static final String AVERAGE_POSTS_PER_USER = "avg.posts.per.user";
        private double average = 0.0;
        private MultipleOutputs<Text, Text> mos = null;
        private Text outKey = new Text(), outValue = new Text();
        private Map<String, String> userIdToReputation = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            average = getAveragePostsPerUser(context.getConfiguration());
            mos = new MultipleOutputs<>(context);
            URI[] files = context.getCacheFiles();
            for (URI uri : files) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(uri)))));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    Map<String, String> parsed = MRDPUtils.transFormXMLToMap(line);
                    userIdToReputation.put(parsed.get("Id"), parsed.get("Reputation"));
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");

            String userId = tokens[0];
            int posts = Integer.parseInt(tokens[1]);
            outKey.set(userId);
            outValue.set((long) posts + "\t" + userIdToReputation.get(userId));

            if ((double) posts < average) {
                mos.write(MULTIPLE_OUTPUTS_BELOW_NAME, outKey, outValue, MULTIPLE_OUTPUTS_BELOW_NAME + "/part");
            } else {
                mos.write(MULTIPLE_OUTPUTS_ABOVE_NAME, outKey, outValue, MULTIPLE_OUTPUTS_ABOVE_NAME + "/part");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        public static void setAveragePostsPerUser(Job job, double averagePostsPerUser) {
            job.getConfiguration().set(AVERAGE_POSTS_PER_USER, Double.toString(averagePostsPerUser));
        }

        public static double getAveragePostsPerUser(Configuration configuration) {
            return Double.parseDouble(configuration.get(AVERAGE_POSTS_PER_USER));
        }
    }

}
