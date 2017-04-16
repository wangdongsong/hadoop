package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * 分区裁剪
 *
 * 按照最后访问日期对Redis实例分区
 *
 * 问题：给定一组用户数据，按照最后访问日期将用户至声望的映射关系交叉分布到6个Redis实例中
 *
 * Created by wangdongsong1229@163.com on 2017/3/28.
 */
public class RedisByLastAccessDataPrititionMRJobRunner extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class RedisKey implements WritableComparable<RedisKey> {

        private int lastAccessMonth = 0;
        private Text field = new Text();

        public int getLastAccessMonth() {
            return lastAccessMonth;
        }

        public void setLastAccessMonth(int lastAccessMonth) {
            this.lastAccessMonth = lastAccessMonth;
        }

        public Text getField() {
            return field;
        }

        public void setField(Text field) {
            this.field = field;
        }

        @Override
        public int compareTo(RedisKey o) {
            if (this.lastAccessMonth == o.lastAccessMonth) {
                return this.field.compareTo(o.field);
            } else {
                return this.lastAccessMonth < o.lastAccessMonth ? -1 : 1;
            }
        }

        @Override
        public String toString() {
            return this.lastAccessMonth + "\t" + this.field.toString();
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.lastAccessMonth);
            this.field.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.lastAccessMonth = in.readInt();
            this.field.readFields(in);
        }
    }

}
