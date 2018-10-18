import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Sorter {
    public static class ReverseMapper extends Mapper<Text, Text, IntWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(Integer.valueOf(value.toString())),key);
        }
    }

    public static class ReverseReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reducer");
            for (Text v : values) {
                context.write(key, v);
            }
        }
    }
}
