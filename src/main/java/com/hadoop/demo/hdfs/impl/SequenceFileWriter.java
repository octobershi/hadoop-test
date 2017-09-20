package com.hadoop.demo.hdfs.impl;

import com.hadoop.demo.hdfs.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class SequenceFileWriter implements Writer {

    @Override
    public void write() {
        //System.setProperty("HADOOP_USER_NAME", "yshi");
        String dest = "hdfs://localhost:8020/user/yshi/common_test_sequence_block";
        Configuration conf = new Configuration();
        try(FileSystem fs = FileSystem.get(URI.create(dest), conf);
            SequenceFile.Writer writer = SequenceFile.createWriter(
                    conf,
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK),
                    SequenceFile.Writer.file(new Path(dest)),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(Text.class))){
            for(int i = 0; i < 100_000; i++){
                writer.append(new Text("user_" + i % 4), new Text("password_" + i % 4));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
