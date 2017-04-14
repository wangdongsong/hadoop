package com.wds.dataalgorithms;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Chapter01 二次排序 MapReduce解决方案
 * 输入：resources/com/wds/dataalgorithms/secondarysort_input.txt
 *
 * Created by wangdongsong1229@163.com on 2017/4/14.
 */
public class SecondarySortMRDriver extends Configured implements Tool  {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SecondarySortMRDriver(), args);
    }

    @Override
    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("Usage: SecondarySortMrDriver <input path> <output path>");
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(SecondarySortMRDriver.class);
        job.setJobName("SecondarySortMRDriver");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(getConf()).delete(outputPath, true);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(SecondarySortMRWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(SecondarySortMRMapper.class);
        job.setReducerClass(SecondarySortMRReducer.class);
        job.setPartitionerClass(SecondarySortMRPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortMrGroupingComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class SecondarySortMRWritable implements Writable, WritableComparable<SecondarySortMRWritable>{

        private Text yearMonth = new Text();
        private Text day = new Text();
        private IntWritable temperature = new IntWritable();

        @Override
        public void write(DataOutput out) throws IOException {
            yearMonth.write(out);
            day.write(out);
            temperature.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            yearMonth.readFields(in);
            day.readFields(in);
            temperature.readFields(in);
        }

        public SecondarySortMRWritable(Text yearMonth, Text day, IntWritable temperature) {
            this.yearMonth = yearMonth;
            this.day = day;
            this.temperature = temperature;
        }

        public SecondarySortMRWritable() {
        }

        /**
         * 比较器控制键的排序顺序
         * @param o
         * @return
         */
        @Override
        public int compareTo(SecondarySortMRWritable o) {
            int comparaeValue = this.yearMonth.compareTo(o.getYearMonth());

            if (comparaeValue == 0) {
                comparaeValue = temperature.compareTo(o.getTemperature());
            }

            //升序
            return comparaeValue;
            //降序
            //return -1 * comparaeValue;
        }

        public Text getYearMonth() {
            return yearMonth;
        }

        public void setYearMonth(Text yearMonth) {
            this.yearMonth = yearMonth;
        }

        public Text getDay() {
            return day;
        }

        public void setDay(Text day) {
            this.day = day;
        }

        public IntWritable getTemperature() {
            return temperature;
        }

        public void setTemperature(IntWritable temperature) {
            this.temperature = temperature;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SecondarySortMRWritable that = (SecondarySortMRWritable) o;
            if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
                return false;
            }
            if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = yearMonth != null ? yearMonth.hashCode() : 0;
            result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("DateTemperaturePair{yearMonth=");
            builder.append(yearMonth);
            builder.append(", day=");
            builder.append(day);
            builder.append(", temperature=");
            builder.append(temperature);
            builder.append("}");
            return builder.toString();
        }
    }

    private static class SecondarySortMRReducer extends Reducer {
    }

    private static class SecondarySortMRMapper extends Mapper {
    }

    private class SecondarySortMRPartitioner extends Partitioner<SecondarySortMRWritable, Text> {

        @Override
        public int getPartition(SecondarySortMRWritable o, Text o2, int numPartitions) {
            //确保分区数非负
            return Math.abs(o.getYearMonth().hashCode() % numPartitions);
        }
    }

    private class SecondarySortMrGroupingComparator extends WritableComparator {

        public SecondarySortMrGroupingComparator() {
            super(SecondarySortMRWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((SecondarySortMRWritable)a).getYearMonth().compareTo(((SecondarySortMRWritable)b).getYearMonth());
        }
    }
}
