import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                String raw = st.nextToken();
                word.set(raw.replaceAll("[.\"?,!]",""));
                context.write(word, one);
            }
        }
    }



    public static class ReverseCombiner extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Combiner");
            for (Text v : values) {
                context.write(v, key);
            }
        }
    }


    public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("combiner");
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job0 = Utility.genJob(
                "1",
                WordCount.class,
                MyMapper.class,
                MyCombiner.class,
                Shared.CountReducer.class,
                Text.class,
                IntWritable.class,
                "input",
                "temp"
        );
        job0.setOutputFormatClass(SequenceFileOutputFormat.class);
        Job job1 = Utility.genJob(
                "1",
                WordCount.class,
                Shared.ReverseMapper.class,
                Shared.ReverseReducer.class,
                Shared.ReverseReducer.class,
                IntWritable.class,
                Text.class,
                "temp",
                "output"
        );
//        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job0.waitForCompletion(true);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }

}
