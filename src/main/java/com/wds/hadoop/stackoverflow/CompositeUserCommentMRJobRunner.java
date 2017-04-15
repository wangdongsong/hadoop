package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * 组合链接
 *
 *
 *
 * 问题：给定两个大的格式化数据集-用户信息和评论，使用用户信息数据来丰富评论信息
 *
 * Created by wangdongsong1229@163.com on 2017/3/26.
 */
public class CompositeUserCommentMRJobRunner extends Configured implements Tool{

    @Override
    public int run(String[] allArgs) throws Exception {
        Configuration conf = new Configuration();
        String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();
        if (args.length != 4) {
            System.err.println("Usage: CompositeJoin <user data> <comment data> <out> [inner|outer]");
            System.exit(1);
        }
        Path userPath = new Path(args[0]);
        Path commentPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        String joinType = args[3];

        JobConf jobConf = new JobConf("CompositeUserCommentMRJobRunner");
//        Job job = Job.getInstance(conf, "CompositeCommentMRJobRunner");
        jobConf.setJarByClass(CompositeUserCommentMRJobRunner.class);
        jobConf.setMapperClass(CompositeUserCommentMapper.class);
        jobConf.setNumReduceTasks(0);
        jobConf.setInputFormat(CompositeInputFormat.class);
        jobConf.set("mapred.join.expr", CompositeInputFormat.compose(joinType, KeyValueTextInputFormat.class, userPath, commentPath));

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        JobClient.runJob(jobConf);

        return 0;
    }

    public static class CompositeUserCommentMapper extends MapReduceBase implements Mapper<Text, TupleWritable, Text, Text> {
        @Override
        public void map(Text key, TupleWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            output.collect((Text)value.get(0), (Text)value.get(1));
        }

    }
}
