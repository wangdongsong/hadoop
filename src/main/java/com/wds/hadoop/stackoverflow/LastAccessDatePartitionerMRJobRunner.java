package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * 问题：给定一组用户信息，按照最近访问日期中的年份信息对记录进行分区，一年对应一个分区
 *
 * Created by wangdongsong1229@163.com on 2017/3/24.
 */
public class LastAccessDatePartitionerMRJobRunner extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setPartitionerClass(LastAccessDatePartitioner.class);
        LastAccessDatePartitioner.setMinLastAccessDate(job, 2008);
        job.setNumReduceTasks(4);
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class LastAccessDateMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        private IntWritable outKey = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String strDate = parsed.get("LastAccessDate");
            Calendar cal = Calendar.getInstance();
            try {
                cal.setTime(frmt.parse(strDate));
                outKey.set(cal.get(Calendar.YEAR));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            context.write(outKey, value);

        }
    }

    public static class LastAccessDatePartitionerReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(t, NullWritable.get());
            }
        }
    }



    public static class LastAccessDatePartitioner extends Partitioner<IntWritable, Text> implements Configurable {

        private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
        private Configuration conf = null;
        private int minLastAccessDateYear = 0;

        public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {
            job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            this.minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
        }

        @Override
        public Configuration getConf() {
            return this.conf;
        }

        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.get() - minLastAccessDateYear;
        }
    }
}
