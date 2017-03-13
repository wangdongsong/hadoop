package com.wds.hadoop.stackoverflow;

import com.wds.hadoop.utils.MRDPUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

/**
 *  针对StackOverflow的评论数据进行统计
 * <a>https://data.stackexchange.com/help</a>下载归档数据
 * Created by wangdongsong1229@163.com on 2017/3/13.
 */
public class CommentWordCountRunner extends Configured implements Tool {

    @Override
    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        if (args.length < 2) {
            System.err.println("Usage: CommentWordCount <in><out>");
            System.exit(2);
        }

        Job job = Job.getInstance(getConf(), "StackOverflow Comment Word Count");
        job.setJarByClass(CommentWordCountRunner.class);
        job.setMapperClass(CommentWordMapper.class);
        job.setCombinerClass(CommentWordReducer.class);
        job.setReducerClass(CommentWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path(args[1]);
        FileSystem fs = path.getFileSystem(getConf());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        job.submit();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new CommentWordCountRunner(), args);
    }

    public static class CommentWordMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //将XML转成Map对象
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            //获取评论的内容
            String txt = parsed.get("Text");

            if (StringUtils.isBlank(txt.trim())) {
                return;
            }

            txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

            //去掉'符号及单个字母
            txt = txt.replaceAll("'", "").replace("[^a-zA-Z]", "");

            StringTokenizer itr = new StringTokenizer(txt);
            while (itr.hasMoreElements()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private class CommentWordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        //代码同Reducer相同
    }

    private class CommentWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
