package com.wds.dataalgorithms;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * Chapter01 二次排序 MapReduce解决方案
 * 输入：resources/com/wds/dataalgorithms/secondarysort_input.txt
 *
 * Created by wangdongsong1229@163.com on 2017/4/14.
 */
public class SecondarySortMRDriver extends Configured implements Tool  {
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

        return 0;
    }
}
