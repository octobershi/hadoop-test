package com.hadoop.test;

import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class HadoopAvroTestApplication extends Configured implements Tool {

    private static final String SCHEMA = "{\"type\": \"record\",\"name\": \"User\", "
            + "\"fields\": ["
            + "{\"name\":\""
            + "user"
            + "\",\"type\":\"string\"},"
            + "{\"name\":\""
            + "pwd"
            + "\", \"type\":\"string\"}]}";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HadoopAvroTestApplication(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(args[0]);
        if (args.length != 3) {
            System.err.println("Usage: MapReduceColorCount <input path> <output path>");
            return -1;
        }
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HadoopAvroTestApplication.class);
        job.setJobName("avro count");

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(AvroCountMapper.class);
        AvroJob.setInputKeySchema(job, (new Schema.Parser().parse(SCHEMA)));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(AvroCountReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AvroCountMapper extends Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {

        @Override
        public void map(AvroKey<User> key, NullWritable value, Context context) throws IOException, InterruptedException {
            CharSequence name = key.datum().getUser();
            context.write(new Text(name.toString()), new IntWritable(1));
        }
    }

    public static class AvroCountReducer extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sum = StreamSupport.stream(value.spliterator(), false).mapToInt(e -> e.get()).reduce(0, Integer::sum);
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Integer>(sum));
        }
    }

    @Data
    private static class User {

        private String user;
        private String pwd;
    }
}
