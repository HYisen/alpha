import com.sun.istack.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Utility {
    public static Job genJob(
            String jobName,
            Class<?> jar,
            @Nullable Class<? extends Mapper> mapper,
            @Nullable Class<? extends Reducer> combiner,
            @Nullable Class<? extends Reducer> reducer,
            Class<?> key,
            Class<?> value,
            String inputPath,
            String outputPath
    ) throws IOException {
        System.setProperty("hadoop.home.dir", "/");//A dummy for local test

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://node0:9000");
//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.hostname","192.168.0.10");

        Job rtn = Job.getInstance(conf, jobName);
//        rtn.setJar("target/original-alpha-1.0-SNAPSHOT.jar");
        rtn.setJarByClass(jar);
        if (mapper != null) {
            rtn.setMapperClass(mapper);
        }
        if (combiner != null) {
            rtn.setCombinerClass(combiner);
        }
        if (reducer != null) {
            rtn.setReducerClass(reducer);
        }
        rtn.setOutputKeyClass(key);
        rtn.setOutputValueClass(value);
        FileInputFormat.addInputPath(rtn, new Path(inputPath));
        FileOutputFormat.setOutputPath(rtn, new Path(outputPath));
        return rtn;
    }

    public static class EqualOnePartitioner extends Partitioner<IntWritable, Text> {
        private static IntWritable one = new IntWritable(1);

        @Override
        public int getPartition(IntWritable intWritable, Text text, int i) {
            return intWritable.equals(one) ? 0 : 1;
        }
    }


    public static void printConf() {
        Configuration conf = new Configuration();
        conf.forEach(v -> System.out.println(v.getKey() + " = " + v.getValue()));
        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("yarn.resourcemanager.address"));
        System.out.println(conf.get("mapreduce.framework.name"));
    }

    public static void main(String[] args) {
        printConf();
    }
}
