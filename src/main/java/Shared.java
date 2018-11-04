/*
 * Copyright (C) 2018 HYisen <alexhyisen@gmail.com>
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class Shared {
    public static class RecoverCountMapper extends Mapper<Text, Text, IntWritable, Text> {
        IntWritable count = new IntWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            count.set(Integer.valueOf(key.toString()));
            context.write(count, value);
        }
    }

    public static class ReverseIntWritableComparator extends WritableComparator {
        public ReverseIntWritableComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, new IntWritable(
                    StreamSupport.stream(values.spliterator(), false)
                            .mapToInt(IntWritable::get)
                            .sum()
            ));
        }
    }
}
