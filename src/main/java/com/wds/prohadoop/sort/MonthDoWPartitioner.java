package com.wds.prohadoop.sort;

import com.wds.prohadoop.utils.AirlineDataUtils;
import com.wds.prohadoop.utils.MonthDoWWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by wangdongsong1229@163.com on 2017/1/15.
 */
public class MonthDoWPartitioner extends Partitioner<MonthDoWWritable, Text> implements Configurable {

    private Configuration conf = null;
    private int indexRange = 0;


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.indexRange = conf.getInt("key.range", getDefaultRange());
    }


    private int getDefaultRange() {
        int minIndex = 0;
        int maxIndex = 11 * 7 + 6;
        int range = (maxIndex - minIndex) +1;
        return range;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int getPartition(MonthDoWWritable key, Text text, int numReduceTasks) {
        return AirlineDataUtils.getCustomPartition(key, indexRange, numReduceTasks);
    }
}
