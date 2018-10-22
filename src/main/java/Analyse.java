import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Analyse {

    public static class ExtractMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private final static Text sample = new Text("sample");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            word.set(words[2]);
            context.write(word, one);
            context.write(sample, one);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Utility.genJob(
                "count",
                Analyse.class,
                Analyse.ExtractMapper.class,
                Shared.CountReducer.class,
                Shared.CountReducer.class,
                Text.class,
                IntWritable.class,
                "/home/alex/code/01",
                "temp/0"
        );
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);

        job = Utility.genJob(
                "reverse",
                Analyse.class,
                InverseMapper.class,
                null,
                null,
                IntWritable.class,
                Text.class,
                "temp/0",
                "temp/1"
        );
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);

        job = Utility.genJob(
                "sort",
                Analyse.class,
                null,
                null,
                null,
                IntWritable.class,
                Text.class,
                "temp/1",
                "output"
        );
        job.setInputFormatClass(SequenceFileInputFormat.class);

//        job.setNumReduceTasks(2);
//        job.setPartitionerClass(Utility.EqualOnePartitioner.class);

        //According to the sample result, 55% of the keys are 1,
        //which means a RuntimeError "Split points are out of order"
        //would be thrown once NumReduce > 2 with TotalOrderPartitioner.
        job.setNumReduceTasks(20);
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.05, 10000);
        InputSampler.writePartitionFile(job,sampler);
        job.setPartitionerClass(Shared.MyTotalOrderPartitioner.class);
        URI uri = new URI(Shared.MyTotalOrderPartitioner.getPartitionFile(job.getConfiguration()));
        System.out.println(uri);
        job.addCacheFile(uri);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
