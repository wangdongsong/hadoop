package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by wangdongsong1229@163.com on 2017/3/21.
 */
public class CountNumUsersByStateMRJobRunner extends Configured implements Tool {

    @Override
    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        if (args == null || args.length < 2) {
            System.err.println("Usage: CountNumUsersByStateMRJobRunner <in> <out>");
            return 0;
        }
        Job job = Job.getInstance(getConf());
        job.setMapperClass(CountNumUsersByStateMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();
        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        FileSystem.get(getConf()).delete(new Path(args[1]), true);
        System.exit(code);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CountNumUsersByStateMRJobRunner(), args);
    }

    public static class CountNumUsersByStateMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
        public static final String STATE_COUNTER_GROUP ="State";
        public static final String UNKNOWN_COUNTER = "Unknown";
        public static final String NULL_OR_EMPTY_COUNTER = "Nll or Empty";

        private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR",
                "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
                "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
                "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
                "OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
                "VT", "VA", "WA", "WV", "WI", "WY" };

        private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());

            //获取位置
            String location = parsed.get("Location");

            //判断位置是否为空并查找缩写
            if (location != null && !location.isEmpty()) {
                String[] tokens = location.toUpperCase().split("\\s");
                boolean unknown = true;

                for (String state : tokens) {
                    if (states.contains(state)) {
                        context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
                        unknown = false;
                        break;
                    }
                }

                if (unknown) {
                    context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
                }
            } else {
                context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
            }
        }
    }

}
