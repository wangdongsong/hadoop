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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 对输出的列进行where条件选择，选择到推迟到起点，推迟在目的地，推迟发生在起点和目的地
 *
 * 对输入参数进行初始化
 *
 * Created by wangdongsong1229@163.com on 2017/1/7.
 */
public class WhereClauseMRJob extends Configured implements Tool {

    public static final Logger LOGGER = LoggerFactory.getLogger(WhereClauseMRJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new SelectClauseMRJob(), args);
    }

    @Override
    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WhereClauseMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WhereClauseMapper.class);
        job.setNumReduceTasks(0);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);

        return status ? 0: 1;
    }

    public static class WhereClauseMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private int delayInMinutes = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            LOGGER.info(value.toString());

            if (AirlineDataUtils.isHeader(value)) {
                return;
            }

            String[] arr = AirlineDataUtils.getSelectResultsPerRow(value);
            String depDel = arr[8];
            String arrDel = arr[9];

            int iDepDel = AirlineDataUtils.parseMinutes(depDel, 0);
            int iArrDel = AirlineDataUtils.parseMinutes(arrDel, 0);

            StringBuilder out = AirlineDataUtils.mergeStringArray(arr, ",");

            if (iDepDel >= this.delayInMinutes && iArrDel >= this.delayInMinutes) {
                out.append(",").append("B");
                context.write(NullWritable.get(), new Text(out.toString()));
            } else if (iDepDel > this.delayInMinutes) {
                out.append(",").append("O");
                context.write(NullWritable.get(), new Text(out.toString()));
            } else if (iArrDel > this.delayInMinutes) {
                out.append(",").append("D");
                context.write(NullWritable.get(), new Text(out.toString()));
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.delayInMinutes = context.getConfiguration().getInt("map.where.delay", 1);
        }
    }
}
