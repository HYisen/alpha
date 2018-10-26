import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import utility.Stopwatch;

import java.io.IOException;
import java.net.URI;
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

    //WARNING, do not use identical name for different tasks.
    public static void go(String name, Function<String, String> extractor, String inputPath, String outputPath)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Stopwatch stopwatch = new Stopwatch();
        ExtractMapper.extractor = extractor;
        Job job = Utility.genJob(
                name+"_count",
                Analyse.class,
                Analyse.ExtractMapper.class,
                Shared.CountReducer.class,
                Shared.CountReducer.class,
                Text.class,
                IntWritable.class,
                inputPath,
                "temp/"+name+"/0"
        );
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);

        job = Utility.genJob(
                name+"_reverse",
                Analyse.class,
                InverseMapper.class,
                null,
                null,
                IntWritable.class,
                Text.class,
                "temp/"+name+"/0",
                "temp/"+name+"/1"
        );
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);

        job = Utility.genJob(
                name+"_sort",
                Analyse.class,
                null,
                null,
                null,
                IntWritable.class,
                Text.class,
                "temp/"+name+"/1",
                outputPath
        );
        job.setInputFormatClass(SequenceFileInputFormat.class);

//        job.setNumReduceTasks(2);
//        job.setPartitionerClass(Utility.EqualOnePartitioner.class);

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

        job.waitForCompletion(true);
        stopwatch.report(name);
    }

    public static void go(String name, Function<String, String> extractor)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        go(name, extractor, "/home/alex/code/00", "output/" + name);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        go("keyword", v -> v.split("\t")[2]);
    }
}
