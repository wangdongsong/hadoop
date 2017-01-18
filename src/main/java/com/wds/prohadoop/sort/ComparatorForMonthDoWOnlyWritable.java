package com.wds.prohadoop.sort;

import com.wds.prohadoop.utils.MonthDoWOnlyWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;

/**
 * Created by wangdongsong1229@163.com on 2017/1/15.
 */
public class ComparatorForMonthDoWOnlyWritable implements RawComparator {

    MonthDoWOnlyWritable first = null;
    MonthDoWOnlyWritable second = null;
    DataInputBuffer buffer = null;

    public ComparatorForMonthDoWOnlyWritable() {
        first = new MonthDoWOnlyWritable();
        second = new MonthDoWOnlyWritable();
        buffer = new DataInputBuffer();
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            buffer.reset(b1, s1, l1);
            first.readFields(buffer);

            buffer.reset(b2, s2, l2);
            second.readFields(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return this.compare(first, second);
    }

    @Override
    public int compare(Object o1, Object o2) {
        MonthDoWOnlyWritable first = (MonthDoWOnlyWritable) o1;
        MonthDoWOnlyWritable second = (MonthDoWOnlyWritable) o2;
        if (first.month.get() != second.month.get()) {
            return first.month.compareTo(second.month);
        }

        if (first.dayOfWeek.get() != second.dayOfWeek.get()) {
            return -1 * first.dayOfWeek.compareTo(second.dayOfWeek);
        }

        return 0;
    }
}
