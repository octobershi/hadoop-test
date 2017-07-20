package com.hadoop.demo;

import com.hadoop.demo.input.WholeFileInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class HadoopWholeFileInputFormatTestApplication extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(HadoopWholeFileInputFormatTestApplication.class);

	public static class CommonMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

        private Text fileInputName;

        @Override
        public void setup(Context context){
            Path file = ((FileSplit)context.getInputSplit()).getPath();
            fileInputName = new Text(file.getName());
        }

	    @Override
        public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(fileInputName, value);
        }
    }

    public static class CommonReducer extends Reducer<Text, BytesWritable, Text, IntWritable> {

	    private static IntWritable result = new IntWritable();

	    @Override
        public void reduce(Text key, Iterable<BytesWritable> value, Context context) {
            result.set(StreamSupport.stream(value.spliterator(), true).mapToInt(v -> v.getLength()).sum());
            try {
                context.write(key, result);
            } catch (IOException e) {
                context.setStatus("this status means IOException - " + key);
            } catch (InterruptedException e) {
                context.setStatus("this status means InterruptedException - " + key);
            }
            context.setStatus("this status means everything is going well");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MapReduceColorCount <input path> <output path>");
            return -1;
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = getConf();
        //N.B.  if directly run with main(), the fs needs to be explicitly assigned to hdfs,
        //      and the command line args are needed.
        conf.set("fs.defaultFS", "hdfs://localhost/");
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        //conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
        //conf.setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);

        Job job = Job.getInstance(conf, "common text");
        job.setJarByClass(HadoopWholeFileInputFormatTestApplication.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setMapperClass(CommonMapper.class);
        job.setCombinerClass(CommonReducer.class);
        job.setReducerClass(CommonReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        WholeFileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	    int result = ToolRunner.run(new HadoopWholeFileInputFormatTestApplication(), args);
	    log.debug("System exits with status " + result);
        System.exit(result);
    }
}
