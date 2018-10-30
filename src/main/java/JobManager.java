import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Something like(not is) a builder.
 * Exception from chaining its original setter,
 * it also save some identical information through constructors,
 * which reduce the redundant identical setting procedure,
 * which can also be accomplished through a static method in Utility.
 * In fact, JobManager is just an refrain of Utility.genJob() at the very beginning.
 */
public class JobManager {
    private Job job;

    private String firstName;
    private Class<?> jarClass;
    private boolean isRemote;

    static {
        System.setProperty("hadoop.home.dir", "/");//A dummy for local test
    }

    public JobManager(String firstName, Class<?> jarClass, boolean isRemote) {
        this.firstName = firstName;
        this.jarClass = jarClass;
        this.isRemote = isRemote;
    }

    public JobManager(String firstName, Class<?> jarClass) {
        this(firstName, jarClass, false);
    }

    public JobManager newJob(String secondName) throws IOException {
        Configuration conf = new Configuration();
        if (isRemote) {
            conf.set("fs.defaultFS", "hdfs://node0:9000");
            conf.set("mapreduce.framework.name", "yarn");
            conf.set("yarn.resourcemanager.hostname", "192.168.0.10");
        }
        job = Job.getInstance(conf, firstName + "_" + secondName);
        job.setJarByClass(jarClass);
        if (isRemote) {
            job.setJar("target/original-alpha-1.0-SNAPSHOT.jar");
        }
        return this;
    }

    public JobManager mapperClass(Class<? extends Mapper> orig) {
        job.setMapperClass(orig);
        return this;
    }

    public JobManager combinerClass(Class<? extends Reducer> orig) {
        job.setCombinerClass(orig);
        return this;
    }

    public JobManager reducerClass(Class<? extends Reducer> orig) {
        job.setReducerClass(orig);
        return this;
    }

    public JobManager outputKeyClass(Class<?> orig) {
        job.setOutputKeyClass(orig);
        return this;
    }

    public JobManager outputValueClass(Class<?> orig) {
        job.setOutputValueClass(orig);
        return this;
    }

    public JobManager inputPath(String orig) throws IOException {
        FileInputFormat.addInputPath(job, new Path(orig));
        return this;
    }

    public JobManager outputPath(String orig) {
        FileOutputFormat.setOutputPath(job, new Path(orig));
        return this;
    }

    public JobManager inputFormat(Class<? extends InputFormat> orig) {
        job.setInputFormatClass(orig);
        return this;
    }

    public JobManager outputFormat(Class<? extends OutputFormat> orig) {
        job.setOutputFormatClass(orig);
        return this;
    }

    public JobManager sortComparatorClass(Class<? extends RawComparator> orig) {
        job.setSortComparatorClass(orig);
        return this;
    }

    public JobManager groupingComparatorClass(Class<? extends RawComparator> orig) {
        job.setGroupingComparatorClass(orig);
        return this;
    }

    public Job getJob() {
        return job;
    }
}
