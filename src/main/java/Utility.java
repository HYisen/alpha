import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Utility {
    public static Job genJob(
            String jobName,
            Class<?> jar,
            Class<? extends Mapper> mapper,
            Class<? extends Reducer> combiner,
            Class<? extends Reducer> reducer,
            Class<?> key,
            Class<?> value,
            String inputPath,
            String outputPath
    ) throws IOException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs:///node0:9000");//prefer fs.defaultFS
//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.address","192.168.0.10");
//        conf.set("fs.defaultFS", "hdfs://node0:9000");
//        conf.set("hadoop.job.user", "alex");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("mapreduce.jobtracker.address", "node0:9001");
//        conf.set("yarn.resourcemanager.hostname", "node0");
//        conf.set("mapreduce.jobhistory.address", "node0:10020");

        Job rtn = Job.getInstance(conf, jobName);
        rtn.setJarByClass(jar);
        rtn.setMapperClass(mapper);
        rtn.setCombinerClass(combiner);
        rtn.setReducerClass(reducer);
        rtn.setOutputKeyClass(key);
        rtn.setOutputValueClass(value);
        FileInputFormat.addInputPath(rtn, new Path(inputPath));
        FileOutputFormat.setOutputPath(rtn, new Path(outputPath));
        return rtn;
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
