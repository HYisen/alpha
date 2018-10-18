import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Dropper {
    public static double calc(int total) {
        Random rand = new Random();
        int count = 0;
        for (int i = 0; i < total; i++) {
            double x = rand.nextDouble();
            double y = rand.nextDouble();
//            System.out.println(String.format("(%5f,%5f)", x, y));
            if (x * x + y * y < 1) {
                count++;
            }
        }
        double pi = 4.0 * count / total;
//        System.out.println(pi);
        return pi;
    }

    public static class MyMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int weight = Integer.valueOf(value.toString());
            context.write(new IntWritable(weight), new DoubleWritable(calc(weight * 1000000)));
        }
    }

    public static class MyReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<DoubleWritable> data = StreamSupport.stream(values.spliterator(), false)
                    .collect(Collectors.toList());
            double average = data.stream()
                    .mapToDouble(DoubleWritable::get)
                    .map(v -> v / data.size())
                    .sum();
            int size = data.size() * key.get();
            System.out.println(size + " -> " + average);
            context.write(new IntWritable(size), new DoubleWritable(average));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Utility.genJob(
                "PI",
                Dropper.class,
                Dropper.MyMapper.class,
                Dropper.MyReducer.class,
                Dropper.MyReducer.class,
                IntWritable.class,
                DoubleWritable.class,
                "config",
                "output"
        );
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

