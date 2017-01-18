package com.wds.prohadoop.sort;

import com.wds.prohadoop.utils.AirlineDataUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wangdongsong1229@163.com on 2017/1/15.
 */
public class AnalyzeConsecutiveArrivalDelaysMRJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }

    public static class ArrivalFlightKey implements WritableComparable<ArrivalFlightKey> {

        public Text destinationAirport = new Text("");
        public Text arrivalDtTime = new Text("");

        public ArrivalFlightKey() {
        }

        public ArrivalFlightKey(Text destinationAirport, Text arrivalDtTime) {
            this.destinationAirport = destinationAirport;
            this.arrivalDtTime = arrivalDtTime;
        }

        @Override
        public int hashCode() {
            return (this.destinationAirport).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ArrivalFlightKey)) {
                return false;
            }
            ArrivalFlightKey other = (ArrivalFlightKey) obj;
            return this.destinationAirport.equals(other.destinationAirport);
        }

        @Override
        public int compareTo(ArrivalFlightKey o) {
            return this.destinationAirport.compareTo(o.destinationAirport);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.destinationAirport.write(out);
            this.arrivalDtTime.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.destinationAirport.readFields(in);
            this.arrivalDtTime.readFields(in);
        }
    }

    public static class ArrivalFlightKeyBasedPartitioner extends Partitioner<ArrivalFlightKey, Text> {
        @Override
        public int getPartition(ArrivalFlightKey arrivalFlightKey, Text text, int numPartitions) {
            return Math.abs(arrivalFlightKey.destinationAirport.hashCode() % numPartitions);
        }
    }

    public class ArrivalFlightKeySortingComparator extends WritableComparator {

        public ArrivalFlightKeySortingComparator() {
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ArrivalFlightKey first = (ArrivalFlightKey) a;
            ArrivalFlightKey second = (ArrivalFlightKey) b;
            if (first.destinationAirport.equals(second.destinationAirport)) {
                return first.arrivalDtTime.compareTo(second.arrivalDtTime);
            } else {
                return first.destinationAirport.compareTo(second.destinationAirport);
            }
        }
    }

    public static class ArrivalFlightKeyGroupingComparator extends WritableComparator {
        public ArrivalFlightKeyGroupingComparator() {
            super(ArrivalFlightKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ArrivalFlightKey first = (ArrivalFlightKey) a;
            ArrivalFlightKey second = (ArrivalFlightKey) b;

            return first.destinationAirport.compareTo(second.destinationAirport);
        }
    }

    public static class AnalyzeConsecutiveDelaysMapper extends Mapper<LongWritable, Text, ArrivalFlightKey, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                String[] contents = value.toString().split(",");
                String arrivingAirport = AirlineDataUtils.getDestination(contents);
                String arrivingDtTime = AirlineDataUtils.getArrivalTime(contents);

                int arrivalDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents), 0);
                if (arrivalDelay > 0) {
                    ArrivalFlightKey afKey = new ArrivalFlightKey(new Text(arrivingAirport), new Text(arrivingDtTime));
                    context.write(afKey, value);
                }
            }
        }
    }

    public static class AnalyzeConsecutiveDelayReducer extends Reducer<ArrivalFlightKey, Text, NullWritable, Text> {
        @Override
        protected void reduce(ArrivalFlightKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text previousRecord = null;
            for (Text v : values) {
                StringBuilder out = new StringBuilder("");
                if (previousRecord == null) {
                    out.append(v.toString()).append("|");
                } else {
                    out.append(v.toString()).append("|").append(previousRecord.toString());
                }

                context.write(NullWritable.get(), new Text(out.toString()));
                previousRecord = new Text(v.toString());
            }
        }
    }



}
