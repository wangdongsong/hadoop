package com.wds.prohadoop.join;

import com.wds.prohadoop.utils.AirlineDataUtils;
import com.wds.prohadoop.utils.DelaysWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by wangdongsong1229@163.com on 2017/1/17.
 */
public class JoinMRJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new JoinMRJob(), args);
    }

    @Override
    public int run(String[] allArgs) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(JoinMRJob.class);

        job.setPartitionerClass(CarrierCodeBasedPartitioner.class);
        job.setGroupingComparatorClass(CarrierGroupComparator.class);
        job.setSortComparatorClass(CarrierSortComparator.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(CarrierKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(JoinReducer.class);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CarrierMasterMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.submit();
        job.waitForCompletion(true);
        return 0;
    }

    public class CarrierMasterMapper extends Mapper<LongWritable, Text, CarrierKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!AirlineDataUtils.isCarrierFileHeader(value.toString())) {
                String[] carrierDetails = AirlineDataUtils.parseCarrierLine(value.toString());
                Text carrierCode = new Text(carrierDetails[0].toLowerCase().trim());
                Text desc = new Text(carrierDetails[1].toUpperCase().trim());
                CarrierKey ck = new CarrierKey(CarrierKey.TYPE_CARRIER, carrierCode, desc);
                context.write(ck, new Text());
            }
        }
    }

    public  class FlightDataMapper extends Mapper<LongWritable, Text, CarrierKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                String[] content = value.toString().split(",");
                String carrierCode = AirlineDataUtils.getUniqueCarrier(content);
                Text code = new Text(carrierCode.toLowerCase().trim());
                CarrierKey carrierKey = new CarrierKey(CarrierKey.TYPE_DATA, code);
                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(value.toString());
                Text dwText = AirlineDataUtils.parseDelaysWritableToText(dw);
                context.write(carrierKey, dwText);

            }
        }
    }

    public class JoinReducer extends Reducer<CarrierKey, Text, NullWritable, Text> {
        @Override
        protected void reduce(CarrierKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String carrierDesc = "UNKNOWN";
            for (Text v : values) {
                if (key.type.equals(CarrierKey.TYPE_CARRIER)) {
                    carrierDesc = key.desc.toString();
                    continue;
                } else {
                    Text out = new Text(v.toString() + ", " + carrierDesc);
                    context.write(NullWritable.get(), out);
                }
            }
        }
    }


    public class CarrierCodeBasedPartitioner extends Partitioner<CarrierKey, Text> {
        @Override
        public int getPartition(CarrierKey key, Text value, int numPartitions) {
            return Math.abs(key.code.hashCode() % numPartitions);
        }
    }

    public class CarrierSortComparator extends WritableComparator {
        public CarrierSortComparator() {
            super(CarrierKey.class, true);
        }
    }

    public class CarrierGroupComparator extends WritableComparator {
        public CarrierGroupComparator() {
            super(CarrierKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((CarrierKey) a).code.compareTo(((CarrierKey) b).code);
        }
    }









}
