package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import sun.security.krb5.Config;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * 布隆过滤
 * 问题：给定一个用户评论的列表，过滤掉大部分没有包含特定关键词的评论。
 * 说明：下面代码使用一个预先确定的词集合产生一个布隆过滤器。是一个通用的应用程序，输入参数为一个gzip输入文件或一个gzip文件目录、
 * 文件中的元素数目、一个可以容忍的误判率真以及输出的文件名
 * Created by wangdongsong1229@163.com on 2017/3/22.
 */
public class BloomFilterMRJobRunner extends Configured implements Tool {


    @Override
    public int run(String[] allArgs) throws Exception {
        Configuration conf = new Configuration();
        String[] args = new GenericOptionsParser(conf, allArgs)
                .getRemainingArgs();
        if (args.length != 4) {
            System.err.println("Usage: BloomFiltering <in> <cachefile> <out><hbase>");
            System.exit(1);
        }

        FileSystem.get(conf).delete(new Path(args[2]), true);

        Job job = new Job(conf, "StackOverflow Bloom Filtering");
        job.setJarByClass(BloomFilterMRJobRunner.class);
        if ("Hbase".equalsIgnoreCase(args[3])) {
            job.setMapperClass(BloomFilteringMapper.class);
        } else {
            job.setMapperClass(BloomFilterMapper.class);
        }
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        DistributedCache.addCacheFile(
                FileSystem.get(conf).makeQualified(new Path(args[1]))
                        .toUri(), job.getConfiguration());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        //commonBloomFilter();
        ToolRunner.run(new BloomFilterMRJobRunner(), args);

    }

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
        private BloomFilter filter = new BloomFilter();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
            System.out.println("Reading Bloom filter from:" + files[0].getPath());

            DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));

            filter.readFields(strm);
            strm.close();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            String comment = parsed.get("Text");
            StringTokenizer tokenizer = new StringTokenizer(comment);
            while (tokenizer.hasMoreTokens() ) {
                String word = tokenizer.nextToken();
                if (filter.membershipTest(new Key(word.getBytes()))) {
                    context.write(value, NullWritable.get());
                    break;
                }
            }
        }
    }

    /***
     * 给定一个用户评论的列表，过滤掉声望值小于1500的用户发出的评论
     */
    public static class BloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable> {

        private BloomFilter filter = new BloomFilter();
        private HTable table = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            URI[] files = DistributedCache.getCacheFiles(context
                    .getConfiguration());

            if (files != null && files.length == 1) {System.out.println("Reading Bloom filter from: " + files[0].getPath());

                // Open local file for read.
                DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));

                // Read into our Bloom filter.
                filter.readFields(strm);
                strm.close();
            } else {
                throw new IOException("Bloom filter file not set in the DistributedCache.");
            }

            Configuration hconf = HBaseConfiguration.create();
            table = new HTable(hconf, "user_table");
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse the input into a nice map.
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            // Get the value for the comment
            String userid = parsed.get("UserId");

            // If it is null, skip this record
            if (userid == null) {
                return;
            }

            // If this user ID is in the set
            if (filter.membershipTest(new Key(userid.getBytes()))) {
                // Get the reputation from the HBase table
                Result r = table.get(new Get(userid.getBytes()));
                int reputation = Integer.parseInt(new String(r.getValue(
                        "attr".getBytes(), "Reputation".getBytes())));
                // If the reputation is at least 1,500,
                // write the record to the file system
                if (reputation >= 1500) {
                    context.write(value, NullWritable.get());
                }
            }
        }
    }

}
