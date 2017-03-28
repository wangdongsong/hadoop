package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 问题：给定一组用户信息，采用并行的方式随机地将用户与声望的映射关系分布到数量可配置的Redis实例中。
 * Created by wangdongsong1229@163.com on 2017/3/27.
 */
public class RedisOutputMRJobRunner extends Configured implements Tool {

    @Override
    public int run(String[] allArgs) throws Exception {

        String args[] = new GenericOptionsParser(allArgs).getRemainingArgs();

        Configuration conf = new Configuration();

        if (args.length != 3) {
            System.err.println("Usage: RedisInputMRJobRunner <user data> <redis hosts> <hash name>");
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        String hosts = args[1];
        String hashName = args[2];

        Job job = new Job(conf, "Redis Output");
        job.setJarByClass(RedisOutputMRJobRunner.class);

        job.setMapperClass(RedisOutputMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);

        job.setOutputFormatClass(RedisHashOutputFormat.class);
        RedisHashOutputFormat.setRedisHostsConf(job, hosts);
        RedisHashOutputFormat.setRedisHashKeyConf(job, hashName);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        int code = job.waitForCompletion(true) ? 0 : 2;

        return code;
    }

    public static void main(String[] args) {

    }

    public static class RedisOutputMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String userId = parsed.get("Id");
            String reputation = parsed.get("Reputation");

            outKey.set(userId);
            outValue.set(reputation);

            context.write(outKey, outValue);
        }
    }




    public static class RedisHashOutputFormat extends OutputFormat<Text, Text> {
        public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
        public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

        public static void setRedisHostsConf(Job job, String hosts) {
            job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
        }

        public static void setRedisHashKeyConf(Job job, String hashKey) {
            job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
        }

        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            return new RedisHashRecordWriter(job.getConfiguration().get(
                    REDIS_HASH_KEY_CONF), job.getConfiguration().get(
                    REDIS_HOSTS_CONF));
        }

        @Override
        public void checkOutputSpecs(JobContext job)
                throws IOException {
            String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);

            if (hosts == null || hosts.isEmpty()) {
                throw new IOException(REDIS_HOSTS_CONF
                        + " is not set in configuration.");
            }

            String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);

            if (hashKey == null || hashKey.isEmpty()) {
                throw new IOException(REDIS_HASH_KEY_CONF
                        + " is not set in configuration.");
            }
        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
        }


    }

    public static class RedisHashRecordWriter extends RecordWriter<Text, Text> {

        private Map<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
        private String hashKey = null;

        public RedisHashRecordWriter(String hashKey, String hosts) {
            System.out.println("Connecting to " + hosts + " and writing to " + hashKey);
            this.hashKey = hashKey;
            // Create a connection to Redis for each host
            // Map an integer 0-(numRedisInstances - 1) to the instance
            int i = 0;
            for (String host : hosts.split(",")) {
                Jedis jedis = new Jedis(host);
                jedis.connect();
                jedisMap.put(i, jedis);
                ++i;
            }
        }

        @Override
        public void write(Text key, Text value) throws IOException,
                InterruptedException {
            // Get the Jedis instance that this key/value pair will be
            // written to
            Jedis j = jedisMap.get(Math.abs(key.hashCode())
                    % jedisMap.size());

            // Write the key/value pair
            j.hset(hashKey, key.toString(), value.toString());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            // For each jedis instance, disconnect it
            for (Jedis jedis : jedisMap.values()) {
                jedis.disconnect();
            }
        }
    }

}
