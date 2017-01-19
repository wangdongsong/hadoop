package com.wds.prohadoop.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wangdongsong1229@163.com on 2017/1/17.
 */
public class CarrierKey implements WritableComparable<CarrierKey> {

    public static final IntWritable TYPE_CARRIER = new IntWritable(0);
    public static final IntWritable TYPE_DATA = new IntWritable(1);
    public IntWritable type = new IntWritable(3);
    public Text code = new Text("");
    public Text desc = new Text("");

    public CarrierKey() {
    }

    public CarrierKey(IntWritable type, Text code, Text desc) {
        this.type = type;
        this.code = code;
        this.desc = desc;
    }

    public CarrierKey(IntWritable type, Text code) {
        this.type = type;
        this.code = code;
    }

    @Override
    public int compareTo(CarrierKey o) {
        CarrierKey first = this;
        if (first.code.equals(o.code)) {
            return first.type.compareTo(o.type);
        } else {
            return first.code.compareTo(o.code);
        }
    }

    @Override
    public int hashCode() {
        return (this.code.toString() + Integer.toString(this.type.get())).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CarrierKey)) {
            return false;
        } else {
            return (this.code.equals(((CarrierKey) obj).code) && this.type.equals(((CarrierKey) obj).type));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.type.write(out);
        this.code.write(out);
        this.desc.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.type.readFields(in);
        this.code.readFields(in);
        this.desc.readFields(in);
    }
}
