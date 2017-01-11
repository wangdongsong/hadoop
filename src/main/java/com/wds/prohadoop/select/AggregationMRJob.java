package com.wds.prohadoop.select;

import com.wds.prohadoop.utils.AirlineDataUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by wangdongsong1229@163.com on 2017/1/8.
 */
public class AggregationMRJob extends Configured implements Tool {

    public static final IntWritable RECORD=new IntWritable(0);
    public static final IntWritable ARRIVAL_DELAY=new IntWritable(1);
    public static final IntWritable ARRIVAL_ON_TIME=new IntWritable(2);
    public static final IntWritable DEPARTURE_DELAY=new IntWritable(3);
    public static final IntWritable DEPARTURE_ON_TIME=new IntWritable(4);
    public static final IntWritable IS_CANCELLED=new IntWritable(5);
    public static final IntWritable IS_DIVERTED=new IntWritable(6);

    public static class AggregationMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                String[] contents = value.toString().split(",");
                String month = AirlineDataUtils.getMonth(contents);

                int arrivalDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents), 0);
                int departureDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getDepartureDelay(contents), 0);
                boolean isCancelled = AirlineDataUtils.parseBoolean(AirlineDataUtils.getCancelled(contents), false);
                boolean isDiverted = AirlineDataUtils.parseBoolean(AirlineDataUtils.getDiverted(contents), false);

                context.write(new Text(month), RECORD);

                if (arrivalDelay > 0) {
                    context.write(new Text(month), ARRIVAL_DELAY);
                } else{
                    context.write(new Text(month), ARRIVAL_ON_TIME);
                }

                if (departureDelay > 0) {
                    context.write(new Text(month), DEPARTURE_DELAY);
                }else {
                    context.write(new Text(month), DEPARTURE_ON_TIME);
                }

                if (isCancelled) {
                    context.write(new Text(month), IS_CANCELLED);
                }
                if (isDiverted) {
                    context.write(new Text(month), IS_DIVERTED);
                }

            }
        }
    }

    public static class AggregationReucer extends Reducer<Text, IntWritable, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double totalRecords = 0;
            double arrivalOnTime = 0;
            double arrivalDelays = 0;
            double departureOnTime = 0;
            double departureDelays = 0;
            double cancellations = 0;
            double diversions = 0;

            System.out.println("key = " + key);

            for (IntWritable v : values) {
                if (RECORD.equals(v)) {
                    totalRecords++;
                }

                if (ARRIVAL_DELAY.equals(v)) {
                    arrivalDelays++;
                }

                if (ARRIVAL_ON_TIME.equals(v)) {
                    arrivalOnTime++;
                }

                if (DEPARTURE_DELAY.equals(v)) {
                    departureDelays++;
                }

                if (DEPARTURE_ON_TIME.equals(v)) {
                    departureOnTime++;
                }

                if (IS_CANCELLED.equals(v)) {
                    cancellations++;
                }

                if (IS_DIVERTED.equals(v)) {
                    diversions++;
                }
            }

            DecimalFormat df = new DecimalFormat("0.0000");
            StringBuilder output = new StringBuilder(key.toString());
            output.append(",").append(totalRecords);
            output.append(",").append(df.format(arrivalDelays/totalRecords));
            output.append(",").append(df.format(arrivalOnTime/totalRecords));
            output.append(",").append(df.format(departureOnTime/totalRecords));
            output.append(",").append(df.format(departureDelays/totalRecords));
            output.append(",").append(df.format(cancellations/totalRecords));
            output.append(",").append(df.format(diversions/totalRecords));

            context.write(NullWritable.get(), new Text(output.toString()));
        }
    }



    @Override
    public int run(String[] allArgs) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(AggregationMRJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(AggregationMapper.class);
        job.setReducerClass(AggregationReucer.class);
        job.setNumReduceTasks(1);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);

        return status ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new AggregationMRJob(), args);
    }
}
