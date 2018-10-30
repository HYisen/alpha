import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import utility.Stopwatch;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.function.Function;

public class Analyse {
    public static class ExtractMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private final static Text sample = new Text("sample");

        //WARNING, any modification there in runtime is global.
        public static Function<String, String> extractor = Function.identity();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set(extractor.apply(value.toString()));
            context.write(word, one);
            context.write(sample, one);
        }
    }

    public static class LimitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        //WARNING, any modification there in runtime is global.
        public static int limit = 2;
        private int count = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (count < limit) {
//                //WARNING, as Stream::count API changed since Java 11,
//                //count may skip peek if it can get size directly.
//                count += StreamSupport.stream(values.spliterator(), false)
//                        .peek(v -> {
//                            try {
//                                context.write(key, v);
//                            } catch (IOException | InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        })
//                        .count();
                //Identical purpose, but the previous commented one is dirty in exception handling.
                for (Text v : values) {
                    context.write(key, v);
                    count++;
                }
            }
        }
    }

    public static class OccupancyReducer extends Reducer<IntWritable, Text, Text, Text> {
        private int total = 0;

        private Text stats = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (total == 0) {
                //should be the first line that contains sum information
                total = key.get();
                return;
            }
            //I don't use stream for the same reason in LimitReducer.
            for (Text v : values) {
                stats.set(String.format("%f\t%d", (float) (key.get()) / total, key.get()));
                context.write(stats, v);
            }
        }
    }

    //WARNING, do not use identical name for different tasks.
    public static void go(String name, Function<String, String> extractor, String inputPath, String outputPath,
                          int limit, boolean calcOccupancy)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Stopwatch stopwatch = new Stopwatch();
        ExtractMapper.extractor = extractor;
        JobManager manager = new JobManager(name, Analyse.class, false);
        Job job;

        job = manager
                .newJob("count")
                .mapperClass(Analyse.ExtractMapper.class)
                .combinerClass(Shared.CountReducer.class)
                .reducerClass(Shared.CountReducer.class)
                .outputKeyClass(Text.class)
                .outputValueClass(IntWritable.class)
                .inputPath(inputPath)
                .outputPath("temp/" + name + "/0")
                .outputFormat(SequenceFileOutputFormat.class)
                .getJob();
        job.waitForCompletion(true);

        job = manager
                .newJob("reverse")
                .mapperClass(InverseMapper.class)
                .outputKeyClass(IntWritable.class)
                .outputValueClass(Text.class)
                .inputPath("temp/" + name + "/0")
                .outputPath("temp/" + name + "/1")
                .inputFormat(SequenceFileInputFormat.class)
                .outputFormat(SequenceFileOutputFormat.class)
                .getJob();
        job.waitForCompletion(true);

        job = manager
                .newJob("sort")
                .outputKeyClass(IntWritable.class)
                .outputValueClass(Text.class)
                .inputPath("temp/" + name + "/1")
                .outputPath("temp/" + name + "/2")
                .inputFormat(SequenceFileInputFormat.class)
//                .outputFormat(SequenceFileOutputFormat.class)
                .getJob();
        job.setNumReduceTasks(2);
        job.setPartitionerClass(Utility.EqualOnePartitioner.class);

        /*
        //According to the sample result, 55% of the keys are 1,
        //which means a RuntimeError "Split points are out of order"
        //would be thrown once NumReduce > 2 with TotalOrderPartitioner.
        job.setNumReduceTasks(10);
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.05, 10000);
        InputSampler.writePartitionFile(job, sampler);
        job.setPartitionerClass(Shared.MyTotalOrderPartitioner.class);
        URI uri = new URI(Shared.MyTotalOrderPartitioner.getPartitionFile(job.getConfiguration()));
//        System.out.println(uri);
        job.addCacheFile(uri);
        */

        job.waitForCompletion(true);

        LimitReducer.limit = limit + 1;
        job = manager
                .newJob("join")
                .mapperClass(Shared.RecoverCountMapper.class)
                .combinerClass(LimitReducer.class)
                .reducerClass(LimitReducer.class)
                .sortComparatorClass(Shared.ReverseIntWritableComparator.class)
                .outputKeyClass(IntWritable.class)
                .outputValueClass(Text.class)
                .inputPath("temp/" + name + "/2")
                .outputPath(calcOccupancy ? "temp/" + name + "/3" : outputPath)
//                .inputFormat(SequenceFileInputFormat.class
                .inputFormat(KeyValueTextInputFormat.class)
//                .outputFormat(calcOccupancy ? SequenceFileOutputFormat.class : TextOutputFormat.class)
                .getJob();
        job.waitForCompletion(true);

        if (calcOccupancy) {
            job = manager
                    .newJob("stat")
                    .mapperClass(Shared.RecoverCountMapper.class)
                    .reducerClass(OccupancyReducer.class)
                    .sortComparatorClass(Shared.ReverseIntWritableComparator.class)
                    .outputKeyClass(IntWritable.class)
                    .outputValueClass(Text.class)
                    .inputPath("temp/" + name + "/3")
                    .outputPath(outputPath)
//                .inputFormat(SequenceFileInputFormat.class)
                    .inputFormat(KeyValueTextInputFormat.class)
                    .getJob();
            job.waitForCompletion(true);

        }

        stopwatch.report(name);
    }

    public static void go(String name, Function<String, String> extractor, int limit, boolean calcOccupancy)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        go(name, extractor, "/home/alex/code/01", "output/" + name, limit, calcOccupancy);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        go("keyword", v -> v.split("\t")[2], 10, true);
    }
}
