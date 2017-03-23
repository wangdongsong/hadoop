package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Map;

/**
 * Created by wangdongsong1229@163.com on 2017/3/23.
 */
public class DistinceUserMRJobRunner extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text outUserId = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            String userId = parsed.get("UserId");

            outUserId.set(userId);

            context.write(outUserId, NullWritable.get());

        }
    }

    /**
     * 也可以作为Combiner使用
     */
    public static class DistinceReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }


}
