package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 最小值、最大值与计数示例
 * 问题：对于给定的一个用户评论的列表，确定每个用户第一次和最近一次评论的时间以及这个用户所有的评论数
 * Created by wangdongsong1229@163.com on 2017/3/14.
 */
public class MinMaxCountMRJobRunner extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
        private static final Logger MAPPER_LOGGER = LogManager.getLogger(MinMaxCountMapper.class);

        //输出的Key和Vlaue
        private Text outUserId = new Text();
        private MinMaxCountTuple outTuple = new MinMaxCountTuple();
        private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            String strDate = parsed.get("CreationDate");
            String userId = parsed.get("UserId");
            Date creationDate = null;
            try {
                creationDate = frmt.parse(strDate);
            } catch (ParseException e) {
                MAPPER_LOGGER.error(e.getMessage(),e);
            }
            outTuple.setMax(creationDate);
            outTuple.setMin(creationDate);
            outUserId.set(userId);

            context.write(outUserId, outTuple);
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

        private MinMaxCountTuple result = new MinMaxCountTuple();

        @Override
        protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
            result.setMin(null);
            result.setMax(null);
            result.setCount(0);
            int sum = 0;

            for (MinMaxCountTuple val : values) {
                if (result.getMin() == null || val.getMin().compareTo(result.getMin()) < 0) {
                    result.setMin(val.getMin());
                }

                if (result.getMax() == null || val.getMax().compareTo(result.getMax()) > 0) {
                    result.setMax(val.getMax());
                }

                sum += val.getCount();
            }

            result.setCount(sum);
            context.write(key, result);
        }
    }

}
