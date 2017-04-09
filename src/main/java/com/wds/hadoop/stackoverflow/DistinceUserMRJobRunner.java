package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

/**
 * 去重示例
 * 问题：给定一个用户评论的列表，得到去重的用户ID集
 *
 * Created by wangdongsong1229@163.com on 2017/3/23.
 */
public class DistinceUserMRJobRunner extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: UniqueUserCount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "StackOverflow Distinct Users");
        job.setJarByClass(DistinceUserMRJobRunner.class);
        job.setMapperClass(DistinctUserMapper.class);
        job.setCombinerClass(DistinceReducer.class);
        job.setReducerClass(DistinceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DistinceUserMRJobRunner(), args);
    }

    public static class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text outUserId = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            String userId = parsed.get("UserId");

            outUserId.set(userId);

            context.write(outUserId, NullWritable.get());

        }
    }

    /**
     * 也可以作为Combiner使用
     */
    public static class DistinceReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }


}
