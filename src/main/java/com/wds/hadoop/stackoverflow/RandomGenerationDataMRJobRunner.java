package com.wds.hadoop.stackoverflow;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.awt.*;
import java.io.*;
import java.net.URI;
import java.nio.Buffer;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

/**
 * 生成数据模式示例
 *
 * 生成StackOverflow数据，使用1000个单词的列表并且只是生成随机的导语。同时我们也生成随机的分数、行ID、用户ID以及随机创建时间
 *
 * Created by wangdongsong1229@163.com on 2017/3/27.
 */
public class RandomGenerationDataMRJobRunner extends Configured implements Tool {

    public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
    public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
    public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";

    @Override
    public int run(String[] allArgs) throws Exception {

        Configuration conf = new Configuration();
        String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();
        if (args.length != 4) {
            System.err.println("Usage: RandomGenerationDataMRJobRunner <num map tasks> <num records per task> <word list> <output>");
            System.exit(1);
        }

        int numMapTasks = Integer.parseInt(args[0]);
        int numRecordsPerTask = Integer.parseInt(args[1]);
        Path wordList = new Path(args[2]);
        Path outpuPath = new Path(args[3]);

        Job job = Job.getInstance(conf, "RandomGenerationDataMRJobRunner");
        job.setNumReduceTasks(0);
        job.setJarByClass(RandomGenerationDataMRJobRunner.class);
        job.setInputFormatClass(RandomGenerationDataInputFormat.class);

        RandomGenerationDataInputFormat.setNumMapTasks(job, numMapTasks);
        RandomGenerationDataInputFormat.setNumRecordsPerTask(job, numRecordsPerTask);
        RandomGenerationDataInputFormat.setRandomWordList(job, wordList);

        TextOutputFormat.setOutputPath(job, outpuPath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class RandomGenerationDataInputFormat extends InputFormat<Text, NullWritable> {

        @Override
        public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
            int numSplits = context.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            List<InputSplit> splits = new ArrayList<>();
            for (int i = 0; i < numSplits; ++i) {
                splits.add(new FakeInputSplit());
            }
            return splits;
        }

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            RandomDataGeneratorRecordReader rr = new RandomDataGeneratorRecordReader();
            rr.initialize(split, context);
            return rr;
        }

        public static void setNumMapTasks(Job job, int numMapTasks) {
            job.getConfiguration().setInt(NUM_MAP_TASKS, numMapTasks);
        }

        public static void setNumRecordsPerTask(Job job, int numRecordsPerTask) {
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, numRecordsPerTask);
        }

        public static void setRandomWordList(Job job, Path file) {
            job.addCacheFile(file.toUri());
        }

    }

    public static class RandomDataGeneratorRecordReader extends RecordReader<Text, NullWritable>{

        private int numRecordsToCreate = 0;
        private int createdRecords = 0;
        private Text key = new Text();
        private NullWritable value = NullWritable.get();
        private Random rndm = new Random();
        private List<String> randomWords = new ArrayList<String>();
        private SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            this.numRecordsToCreate = context.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);
            URI[] files = context.getCacheFiles();
            BufferedReader br = new BufferedReader(new FileReader(files[0].toString()));
            String line;
            if (files.length == 0) {
                throw new InvalidParameterException("Random word list not set in cache.");
            } else {
                BufferedReader rdr = new BufferedReader(new FileReader(files[0].toString()));
                while ((line = rdr.readLine()) != null) {
                    randomWords.add(line);
                }
                rdr.close();

                if (randomWords.size() == 0) {
                    throw new IOException("Random word list is empty");
                }
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (createdRecords < numRecordsToCreate) {
                // Generate random data
                int score = Math.abs(rndm.nextInt()) % 15000;
                int rowId = Math.abs(rndm.nextInt()) % 1000000000;
                int postId = Math.abs(rndm.nextInt()) % 100000000;
                int userId = Math.abs(rndm.nextInt()) % 1000000;
                String creationDate = frmt
                        .format(Math.abs(rndm.nextLong()));

                // Create a string of text from the random words
                String text = getRandomText();

                String randomRecord = "<row Id=\"" + rowId + "\" PostId=\""
                        + postId + "\" Score=\"" + score + "\" Text=\""
                        + text + "\" CreationDate=\"" + creationDate
                        + "\" UserId\"=" + userId + "\" />";

                key.set(randomRecord);
                ++createdRecords;
                return true;
            } else {
                // Else, return false
                return false;
            }
        }

        @Override
        public Text getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public NullWritable getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) createdRecords / (float) numRecordsToCreate;
        }

        @Override
        public void close() throws IOException {
        }

        public String getRandomText() {
            StringBuilder bldr = new StringBuilder();
            int numWords = Math.abs(rndm.nextInt()) % 30 + 1;

            for (int i = 0; i < numWords; ++i) {
                bldr.append(randomWords.get(Math.abs(rndm.nextInt())
                        % randomWords.size())
                        + " ");
            }
            return bldr.toString();
        }
    }

    public static class FakeInputSplit extends InputSplit implements Writable{

        @Override
        public void readFields(DataInput arg0) throws IOException {
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException,
                InterruptedException {
            return new String[0];
        }
    }
}
