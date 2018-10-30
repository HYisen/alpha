import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
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
            super(IntWritable.class,true);
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

    public static class MyTotalOrderPartitioner<K extends WritableComparable<?>, V> extends Partitioner<K, V> implements Configurable {
        class Position {
            private int offset;
            private int length;

            public Position(int offset) {
                this.offset = offset;
            }

            public Position(int offset, int length) {
                this.offset = offset;
                this.length = length;
            }

            public int getOffset() {
                return offset;
            }

            public int getLength() {
                return length;
            }

            public void setOffset(int offset) {
                this.offset = offset;
            }

            public void setLength(int length) {
                this.length = length;
            }
        }

        private TreeMap<K, Position> info;
        private K[] splitPoints;
        private RawComparator<K> comparator;

        Configuration conf;
        private static final Log LOG = LogFactory.getLog(MyTotalOrderPartitioner.class);

        public MyTotalOrderPartitioner() {
        }

        @SuppressWarnings("unchecked")
        public void setConf(Configuration conf) {
            try {
                this.conf = conf;
                String parts = getPartitionFile(conf);
                Path partFile = new Path(parts);
                FileSystem fs = "_partition.lst".equals(parts) ? FileSystem.getLocal(conf) : partFile.getFileSystem(conf);
                Job job = Job.getInstance(conf);
                Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();

                splitPoints = this.readPartitions(fs, partFile, keyClass, conf);
                if (splitPoints.length != job.getNumReduceTasks() - 1) {
                    throw new IOException("Wrong number of partitions in keyset");
                } else {
                    comparator = (RawComparator<K>) job.getSortComparator();

                    for (int i = 0; i < splitPoints.length - 1; ++i) {
                        if (comparator.compare(splitPoints[i], splitPoints[i + 1]) > 0) {
                            throw new IOException("Split points are out of order");
                        }
                    }

                    info = new TreeMap<>(comparator);

                    int prev = 0;
                    for (int i = 0; i <= splitPoints.length; i++) {
                        if (i == splitPoints.length || !Objects.equals(splitPoints[i], splitPoints[prev])) {
                            info.put(splitPoints[prev], new Position(prev, i - prev));
//                            System.out.println(splitPoints[prev] + " at " + prev + " -> " + i);
                            prev = i;
                        }
                    }

                }
            } catch (IOException var10) {
                throw new IllegalArgumentException("Can't read partitions file", var10);
            }
        }

        public Configuration getConf() {
            return this.conf;
        }

        @SuppressWarnings("unchecked")
        public int getPartition(K key, V value, int numPartitions) {
            int i = Arrays.binarySearch(splitPoints, key, comparator);
            if (i < 0) {
                i = -(i + 1);
                if (i == splitPoints.length) {
                    return splitPoints.length;
                }
            }
            Position pos = info.get(splitPoints[i]);

            return pos.getLength() == 1 ? pos.getOffset() : pos.getOffset() + Math.abs(value.hashCode()) % pos.getLength();
        }

        public static void setPartitionFile(Configuration conf, Path p) {
            conf.set("mapreduce.totalorderpartitioner.path", p.toString());
        }

        public static String getPartitionFile(Configuration conf) {
            return conf.get("mapreduce.totalorderpartitioner.path", "_partition.lst");
        }

        @SuppressWarnings("unchecked")
        private K[] readPartitions(FileSystem fs, Path p, Class<K> keyClass, Configuration conf) throws IOException {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
            ArrayList<K> parts = new ArrayList<>();
            K key = (K) ReflectionUtils.newInstance(keyClass, conf);
            NullWritable value = NullWritable.get();

            try {
                while (reader.next(key, value)) {
                    parts.add(key);
                    key = (K) ReflectionUtils.newInstance(keyClass, conf);
                }

                reader.close();
                reader = null;
            } finally {
                IOUtils.cleanup(LOG, reader);
            }

            return (K[]) parts.toArray((K[]) Array.newInstance(keyClass, parts.size()));
        }
    }
}
