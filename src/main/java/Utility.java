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
        Job rtn = Job.getInstance(new Configuration(), jobName);
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
}
