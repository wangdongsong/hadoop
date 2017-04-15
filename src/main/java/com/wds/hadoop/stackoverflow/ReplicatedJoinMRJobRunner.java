package com.wds.hadoop.stackoverflow;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.awt.*;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;


/**
 * 复制连接
 *
 * 该示例与Reduce端布隆过滤器的复制连接非常类似，通过DistributedCache推送到所有map任务中。将数据直接读入到内存中。
 * 在map阶段就做了连接。
 *
 * 问题：给定一个小的用户信息数据集和一个大的评论数据集，通过用户数据来丰富评论的内容
 *
 * Created by wangdongsong1229@163.com on 2017/3/26.
 */
public class ReplicatedJoinMRJobRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        String joinType = "leftouter";

        if (args.length != 4) {
            System.err.println("Usage: ReplicatedJoinMRJobRunner <user data> <comment data> <out> [inner|leftouter]");
            System.exit(1);
        }

        if ("inner".equalsIgnoreCase(args[3])) {
            joinType = "inner";
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ReplicatedJoinMRJobRunner");
        job.setMapperClass(ReplicatedMapper.class);
        job.getConfiguration().set("jon.type", joinType);
        job.setNumReduceTasks(0);

        TextInputFormat.setInputPaths(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[0]).toUri());

        //DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        //DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);


        return job.waitForCompletion(true) ? 0 : 3;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ReplicatedJoinMRJobRunner(), args);
    }


    public static class ReplicatedMapper extends Mapper<Object, Text, Text, Text> {
        private Text outValue = new Text();
        private String joinType = null;
        private Map<String, String> userIdToInfo = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            URI[] files = context.getCacheFiles();
            for (URI path : files) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(path.toString())))));
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    Map<String, String> parsed = MRDPUtils.transFormXMLToMap(line);
                    String userId = parsed.get("UserId");
                    if (StringUtils.isNotBlank(userId)) {
                        userIdToInfo.put(userId, line);
                    }
                }
            }
            joinType = context.getConfiguration().get("joinType");
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String userId = parsed.get("UserId");
            String userInfo = userIdToInfo.get("userId");

            if (StringUtils.isNotBlank(userInfo)) {
                outValue.set(userInfo);
                context.write(value, outValue);
            } else if (joinType.equalsIgnoreCase("leftouter")) {
                context.write(value, new Text(""));
            }
        }
    }
}

