package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * Created by wangdongsong1229@163.com on 2017/3/14.
 */
public class MinMaxCountTuple implements Writable {
    private Date min = new Date();
    private Date max = new Date();
    private long count = 0;
    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(min.getTime());
        out.writeLong(max.getTime());
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        min = new Date(in.readLong());
        max = new Date(in.readLong());
        count = in.readLong();
    }

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
