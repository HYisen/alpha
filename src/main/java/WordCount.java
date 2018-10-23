import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static class RemovePunctuationMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString().replaceAll("[.\"?,!]", "")), value);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Utility.genJob(
                "1",
                WordCount.class,
                null,
                Shared.CountReducer.class,
                Shared.CountReducer.class,
                Text.class,
                IntWritable.class,
                "input",
                "temp"
        );
        //Obviously, It seems as if ChainMapper would clear the previous Mapper.
        ChainMapper.addMapper(job, TokenCounterMapper.class,
                LongWritable.class, Text.class, Text.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, RemovePunctuationMapper.class,
                Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);

        job = Utility.genJob(
                "1",
                WordCount.class,
                InverseMapper.class,
                null,
                null,
                IntWritable.class,
                Text.class,
                "temp",
                "output"
        );
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(2);
        job.setPartitionerClass(Utility.EqualOnePartitioner.class);
        job.waitForCompletion(true);
    }

}
