package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
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
 * Created by wangdongsong1229@163.com on 2017/3/22.
 */
public class BloomFilterMRJobRunner extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {
        //commonBloomFilter();
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


}
