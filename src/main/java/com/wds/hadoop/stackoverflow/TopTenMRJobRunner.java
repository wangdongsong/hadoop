package com.wds.hadoop.stackoverflow;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Top 10
 * Created by wangdongsong1229@163.com on 2017/3/22.
 */
public class TopTenMRJobRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            String userId = parsed.get("Id");
            String reputation = parsed.get("Reputation");

            repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

            if (repToRecordMap.size() > 0) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : repToRecordMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
                repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")), new Text(value));

                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }

            for (Text t : repToRecordMap.descendingMap().values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

}
