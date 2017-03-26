package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 问题：给定一个用户信息集合和一个用户评论列表，通过为每一条评论添加创建该评论的用户信息来丰富评论的内容
 * Created by wangdongsong1229@163.com on 2017/3/25.
 */
public class ReduceSideJoinMRJobRunner extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ReduceSideJoinMRJobRunner");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentJoinMapper.class);

        job.getConfiguration().set("join.type", args[2]);
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {
        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private List<Text> listA = new ArrayList<>();
        private List<Text> listB = new ArrayList<>();
        private String joinType = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            joinType = context.getConfiguration().get("join.type");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            listA.clear();
            listB.clear();

            values.forEach(tmp ->{
                if (tmp.toString().startsWith("A")) {
                    listA.add(new Text(tmp.toString().substring(1)));
                } else if (tmp.toString().startsWith("B")) {
                    listB.add(new Text(tmp.toString().substring(1)));
                }
            });

            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {
                // If both lists are not empty, join A with B
                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    }
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {
                // For each entry in A,
                for (Text A : listA) {
                    // If list B is not empty, join A and B
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output A by itself
                        context.write(A, new Text(""));
                    }
                }
            } else if (joinType.equalsIgnoreCase("rightouter")) {
                // FOr each entry in B,
                for (Text B : listB) {
                    // If list A is not empty, join A and B
                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output B by itself
                        context.write(new Text(""), B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("fullouter")) {
                // If list A is not empty
                if (!listA.isEmpty()) {
                    // For each entry in A
                    for (Text A : listA) {
                        // If list B is not empty, join A with B
                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                context.write(A, B);
                            }
                        } else {
                            // Else, output A by itself
                            context.write(A, new Text(""));
                        }
                    }
                } else {
                    // If list A is empty, just output B
                    for (Text B : listB) {
                        context.write(new Text(""), B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("anti")) {
                // If list A is empty and B is empty or vice versa
                if (listA.isEmpty() ^ listB.isEmpty()) {

                    // Iterate both A and B with null values
                    // The previous XOR check will make sure exactly one of
                    // these lists is empty and therefore won't have output
                    for (Text A : listA) {
                        context.write(A, new Text(""));
                    }

                    for (Text B : listB) {
                        context.write(new Text(""), B);
                    }
                }
            } else {
                throw new RuntimeException(
                        "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
            }
        }
    }


    private class UserJoinMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String userId = parsed.get("Id");
            outKey.set(userId);
            outValue.set("A" + value.toString());

            context.write(outKey, outValue);
        }
    }

    private class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            outKey.set(parsed.get("UserId"));
            outValue.set("B" + value.toString());

            context.write(outKey, outValue);
        }
    }

    public static class OptimizaUserJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            if (Integer.parseInt(parsed.get("Reputation")) > 1500) {
                outKey.set(parsed.get("Id"));
                outValue.set("A" + value.toString());
                context.write(outKey, outValue);
            }
        }
    }

    public static class OptimizeCommentJoinMapperWithBloom extends Mapper<Object, Text, Text, Text> {
        private BloomFilter bloomFilter = new BloomFilter();
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            DataInputStream strm = new DataInputStream(new FileInputStream(new File(files[0].toString())));
            bloomFilter.readFields(strm);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String userId = parsed.get("UserId");
            if (bloomFilter.membershipTest(new Key(userId.getBytes()))) {
                outKey.set(userId);
                outValue.set("B" + value.toString());
                context.write(outKey, outValue);
            }
        }
    }


}
