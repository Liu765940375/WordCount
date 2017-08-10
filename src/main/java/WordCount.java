/**
 * Created by root on 7/24/17.
 */
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
//    public static class TokenizerMapper
//            extends Mapper<Object, Text, Text, IntWritable> {

//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//
//        public void map(Object key, Text value, Context context
//        ) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
//        }
//    }

//    public static class IntSumReducer
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
////        public void reduce
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//        }
    public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
    public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";

    public static class RandomStackOverflowInputFormat extends
        InputFormat<Text, NullWritable> {

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {

            // Get the number of map tasks configured for
            int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            if (numSplits <= 0) {
                throw new IOException(NUM_MAP_TASKS + " is not set.");
            }

            // Create a number of input splits equivalent to the number of tasks
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i = 0; i < numSplits; ++i) {
                splits.add(new FakeInputSplit());
            }

            return splits;
        }

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(
                InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            // Create a new RandomStackoverflowRecordReader and initialize it
            RandomStackoverflowRecordReader rr = new RandomStackoverflowRecordReader();
            rr.initialize(split, context);
            return rr;
        }

        public static class RandomStackoverflowRecordReader extends
                RecordReader<Text, NullWritable> {

            private Text key = new Text();
            private NullWritable value = NullWritable.get();
            private int numRecordsToCreate = 0;
            private int createdRecords = 0;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                this.numRecordsToCreate = taskAttemptContext.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (createdRecords < numRecordsToCreate) {
                    key.set("bb");
                    ++createdRecords;
                    return true;
                }else{
                    return false;
                }
            }

            @Override
            public void close(){

            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return (float) createdRecords / (float) numRecordsToCreate;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return value;
            }
        }

        /**
         * This class is very empty.
         */
        public static class FakeInputSplit extends InputSplit implements
                Writable {

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

        public static void setNumMapTasks(Job job, int i) {
            job.getConfiguration().setInt(NUM_MAP_TASKS, i);
        }

        public static void setNumRecordPerTask(Job job, int i) {
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bdpe822n3:9000");
//        conf.set("mapreduce.framework.name","yarn");
//        Cluster cluster = new Cluster(conf);
        int numMapTasks = 10;
        int numRecordsPerTask = 10;
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputDir,true);
//        Job job = Job.getInstance(cluster,new JobStatus(),conf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(RandomStackOverflowInputFormat.class);
        RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
        RandomStackOverflowInputFormat.setNumRecordPerTask(job, numRecordsPerTask);
        TextOutputFormat.setOutputPath(job, outputDir);
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
