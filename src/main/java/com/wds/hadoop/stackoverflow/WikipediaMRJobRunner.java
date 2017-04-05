package com.wds.hadoop.stackoverflow;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * 倒序索引示例
 * 问题：给定一组用户评论的信息，创建一个维基百科URL到一组回复贴子的ID的倒序索引
 * Created by wangdongsong1229@163.com on 2017/3/21.
 */
public class WikipediaMRJobRunner extends Configured implements Tool{


    @Override
    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        if (args.length < 3) {
            System.err.println("Usage: MedianStdDevMRJobRunner <in><out><combiner>");
            System.exit(2);
        }

        Job job = Job.getInstance(getConf(), "WikipediaMRJobRunner");
        job.setJarByClass(WikipediaMRJobRunner.class);

        if ("combiner".equalsIgnoreCase(args[3])) {
            job.setCombinerClass(WikipediaReducer.class);
        }
        job.setMapperClass(WikipediaMapper.class);
        job.setReducerClass(WikipediaReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path path = new Path(args[1]);
        FileSystem fs = path.getFileSystem(getConf());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        job.submit();

//        boolean result = job.waitForCompletion(true);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new WikipediaMRJobRunner(), args);
    }

    public static class WikipediaMapper extends Mapper<Object, Text, Text, Text> {

        private Text link = new Text();
        private Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            String txt = parsed.get("Body");
            String postType = parsed.get("PostTypeId");
            String rowId = parsed.get("Id");

            //body为null或者无评论
            if (txt == null || (postType != null && postType.equals("1"))) {
                return;
            }

            txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
            link.set(getWikipediaURL(txt));
            outKey.set(rowId);

            context.write(link, outKey);
        }
    }

    /**
     * Reducer可以和Combiner共用
     */
    public static class WikipediaReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Text id : values) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(id.toString());
            }

            result.set(sb.toString());
            context.write(key, result);

        }
    }

    public static String getWikipediaURL(String text) {

        int idx = text.indexOf("\"http://en.wikipedia.org");
        if (idx == -1) {
            return null;
        }
        int idx_end = text.indexOf('"', idx + 1);

        if (idx_end == -1) {
            return null;
        }

        int idx_hash = text.indexOf('#', idx + 1);

        if (idx_hash != -1 && idx_hash < idx_end) {
            return text.substring(idx + 1, idx_hash);
        } else {
            return text.substring(idx + 1, idx_end);
        }

    }

}
