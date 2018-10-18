import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;

public class Analyse {
    public static class ExtractMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            word.set(words[2]);
            context.write(word, one);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Utility.genJob(
                "count",
                Analyse.class,
                Analyse.ExtractMapper.class,
                Shared.CountReducer.class,
                Shared.CountReducer.class,
                Text.class,
                IntWritable.class,
                "/home/alex/code/00",
                "stats"
        );
        job.waitForCompletion(true);

        job = Utility.genJob(
                "sort",
                Analyse.class,
                Shared.ReverseMapper.class,
                Shared.ReverseReducer.class,
                Shared.ReverseReducer.class,
                IntWritable.class,
                Text.class,
                "stats",
                "output"
        );
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
