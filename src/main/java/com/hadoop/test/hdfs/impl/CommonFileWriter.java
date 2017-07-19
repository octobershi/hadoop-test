package com.hadoop.test.hdfs.impl;

import com.hadoop.test.hdfs.Writer;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class CommonFileWriter implements Writer {

    @Override
    public void write() {
        //System.setProperty("HADOOP_USER_NAME", "yshi");
        String dest = "hdfs://localhost:8020/user/yshi/common_test";
        Configuration conf = new Configuration();
        try(FileSystem fs = FileSystem.get(URI.create(dest), conf);
            OutputStream out = fs.create(new Path(dest), () -> System.out.print("."))){

            for(int i = 0; i < 1_000_000; i++){
                out.write(("user" + i % 7 + "\n").getBytes());
                out.write(("pwd" + i % 7 + "\n").getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
