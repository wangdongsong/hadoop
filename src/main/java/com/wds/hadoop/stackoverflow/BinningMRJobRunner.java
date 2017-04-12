package com.wds.hadoop.stackoverflow;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Map;

/**
 * 分箱模式示例
 *
 * 按照标签将数据过滤到不同的箱子中，分离后，只需要关注特定标签的（Hadoop标签）数据。
 *
 * 问题：给定一组StackOverflow的帖子，按照标签（Hadoop、pig、Hive和Hbase）将帖子分别放到4个箱子中，另外为文本或标题中提及
 * Hadoop的帖子创建一个单独的箱子
 *
 * Created by wangdongsong1229@163.com on 2017/3/24.
 */
public class BinningMRJobRunner extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "bins");
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);

        return 0;
    }

    public static void main(String[] args) {

    }

    public static class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
        private MultipleOutputs<String, NullWritable> mos = null;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            String rawtags = parsed.get("Tags");
            String[] tagTokens = StringEscapeUtils.unescapeHtml(rawtags).split("><");

            for (String tag : tagTokens) {
                String groomed = tag.replaceAll(">|<", "").toLowerCase();
                // If this tag is one of the following, write to the named bin
                if (groomed.equalsIgnoreCase("hadoop")) {
                    mos.write("bins", value, NullWritable.get(), "hadoop-tag");
                }

                if (groomed.equalsIgnoreCase("pig")) {
                    mos.write("bins", value, NullWritable.get(), "pig-tag");
                }

                if (groomed.equalsIgnoreCase("hive")) {
                    mos.write("bins", value, NullWritable.get(), "hive-tag");
                }

                if (groomed.equalsIgnoreCase("hbase")) {
                    mos.write("bins", value, NullWritable.get(), "hbase-tag");
                }
            }

            String post = parsed.get("Body");
            if (post.toLowerCase().contains("hadoop")) {
                mos.write("bins", value, NullWritable.get(), "hadoop-post");
            }
        }


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //必须关闭实例，否则可能完全没有任何输出
            mos.close();
        }
    }

}
