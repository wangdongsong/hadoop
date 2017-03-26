package com.wds.hadoop.stackoverflow;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * 链折叠示例
 * 问题：给定一个用户帖子和用户信息的数据集，依据用户声望是高于5000还是低于5000对用户进行分箱
 * Created by wangdongsong1229@163.com on 2017/3/26.
 */
public class ChainFoldedMRJobRunner extends Configured implements Tool {
    public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
    public static final String MULTIPLE_OUTPUTS_BELOW_5000 = "below5000";
    public static final String MULTIPLE_OUTPUTS_ABOVE_5000 = "above5000";

    @Override
    public int run(String[] allArgs) throws Exception {
        JobConf conf = new JobConf("ChainFoldedMRJobRunner");
        String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();

        if (args.length != 3) {
            System.err.println("Usage: ChainMapperReducer <posts> <users> <out>");
            System.exit(2);
        }

        Path postInput = new Path(args[0]);
        Path userInput = new Path(args[1]);
        Path outputDir = new Path(args[2]);

        // Setup first job to counter user posts
        conf.setJarByClass(ChainFoldedMRJobRunner.class);

        ChainMapper.addMapper(conf, UserIdCountMapper.class,
                LongWritable.class, Text.class, Text.class, LongWritable.class,
                false, new JobConf(false));

        ChainMapper.addMapper(conf, UserIdReputationEnrichmentMapper.class,
                Text.class, LongWritable.class, Text.class, LongWritable.class,
                false, new JobConf(false));

        ChainReducer.setReducer(conf, LongSumReducer.class, Text.class,
                LongWritable.class, Text.class, LongWritable.class, false,
                new JobConf(false));

        ChainReducer.addMapper(conf, UserIdBinningMapper.class, Text.class,
                LongWritable.class, Text.class, LongWritable.class, false,
                new JobConf(false));

        conf.setCombinerClass(LongSumReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.setInputPaths(conf, postInput);

        // Configure multiple outputs
        conf.setOutputFormat(NullOutputFormat.class);
        FileOutputFormat.setOutputPath(conf, outputDir);
        MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_ABOVE_5000,
                TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_BELOW_5000,
                TextOutputFormat.class, Text.class, LongWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        // Add the user files to the DistributedCache
        FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
        for (FileStatus status : userFiles) {
            DistributedCache.addCacheFile(status.getPath().toUri(), conf);
        }

        RunningJob job = JobClient.runJob(conf);

        while (!job.isComplete()) {
            Thread.sleep(5000);
        }

        System.exit(job.isSuccessful() ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class UserIdCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, LongWritable> {
        public static final String RECORDS_COUNTER_NAME = "Records";
        private static final LongWritable ONE = new LongWritable();
        private Text outKey = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            outKey.set(parsed.get("OwnerUserId"));
            output.collect(outKey, ONE);
        }
    }

    public static class UserIdReputationEnrichmentMapper extends MapReduceBase implements Mapper<Text, LongWritable, Text, LongWritable> {
        private Text outKey = new Text();
        private Map<String, String> userIdToReputation = new HashMap<>();

        @Override
        public void configure(JobConf jobConf) {
            try {
                Path[] files = DistributedCache.getLocalCacheFiles(jobConf);
                for (Path p : files) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(p.toString())))));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        Map<String, String> parsed = MRDPUtils.transFormXMLToMap(line.toString());
                        userIdToReputation.put(parsed.get("Id"), parsed.get("Reputation"));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(Text key, LongWritable value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            String reputation = userIdToReputation.get(key.toString());
            if (StringUtils.isNotBlank(reputation)) {
                outKey.set(value.get() + "\t" + reporter);
                output.collect(outKey, value);
            }
        }
    }

    public static class LongSumReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable outValue = new LongWritable();

        @Override
        public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            outValue.set(sum);
            output.collect(key, outValue);
        }
    }

    public static class UserIdBinningMapper extends MapReduceBase implements Mapper<Text, LongWritable, Text, LongWritable> {

        private MultipleOutputs mos = null;

        @Override
        public void configure(JobConf jobConf) {
            mos = new MultipleOutputs(jobConf);
        }

        @Override
        public void map(Text key, LongWritable value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            if (Integer.parseInt(key.toString().split("\t")[1]) < 5000) {
                mos.getCollector(MULTIPLE_OUTPUTS_BELOW_5000, reporter).collect(key, value);
            } else {
                mos.getCollector(MULTIPLE_OUTPUTS_ABOVE_5000, reporter).collect(key, value);
            }

        }

        @Override
        public void close() throws IOException {
            mos.close();
        }
    }
}
