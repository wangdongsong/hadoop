package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Map;

/**
 * Created by wangdongsong1229@163.com on 2017/3/24.
 */
public class TotalOrderMRJobRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path partitionFile = new Path(args[1] + "_partitions.lst");
        Path outputState = new Path(args[1] + "_staging");
        Path outputOrder = new Path(args[1]);

        Job sampleJob = Job.getInstance(conf, "TotalOrderSortingJob");
        sampleJob.setJarByClass(TotalOrderMRJobRunner.class);

        sampleJob.setMapperClass(TotalOrderMapper.class);
        sampleJob.setNumReduceTasks(0);
        sampleJob.setMapOutputKeyClass(Text.class);
        sampleJob.setMapOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(sampleJob, inputPath);

        sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(sampleJob, outputState);

        int code = sampleJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            Job orderJob = Job.getInstance(conf, "TotalOrderJob");
            orderJob.setJarByClass(TotalOrderMRJobRunner.class);

            orderJob.setMapperClass(Mapper.class);
            orderJob.setReducerClass(TotalOrderValue.class);

            orderJob.setNumReduceTasks(10);
            orderJob.setPartitionerClass(TotalOrderPartitioner.class);

            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);

            orderJob.setOutputKeyClass(Text.class);
            orderJob.setOutputValueClass(Text.class);

            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputState);

            TextOutputFormat.setOutputPath(orderJob, outputOrder);

            orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");

            InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(.001, 10000));

            code = orderJob.waitForCompletion(true) ? 0 : 1;
        }
        return code;
    }

    public static void main(String[] args) {

    }


    private class TotalOrderMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            outKey.set(parsed.get("LastAccessDate"));
            context.write(outKey, value);
        }
    }

    private class TotalOrderValue extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(t, NullWritable.get());
            }
        }
    }


}
