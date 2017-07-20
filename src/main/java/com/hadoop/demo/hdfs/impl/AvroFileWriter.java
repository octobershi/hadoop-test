package com.hadoop.demo.hdfs.impl;

import com.hadoop.demo.hdfs.Writer;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;

public class AvroFileWriter implements Writer {

    private static final String SCHEMA = "{\"type\": \"record\",\"name\": \"User\", "
            + "\"fields\": ["
            + "{\"name\":\""
            + "user"
            + "\",\"type\":\"string\"},"
            + "{\"name\":\""
            + "pwd"
            + "\", \"type\":\"string\"}]}";

    @Override
    public void write() {
        //System.setProperty("HADOOP_USER_NAME", "yshi");
        String dest = "hdfs://localhost:8020/user/yshi/avro_test_deflate";
        Configuration conf = new Configuration();
        try(FileSystem fs = FileSystem.get(URI.create(dest), conf);
            OutputStream out = fs.create(new Path(dest), () -> System.out.print("."));
            DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<Object>())){

            Schema schema = new Schema.Parser().parse(SCHEMA);
            writer.setCodec(CodecFactory.deflateCodec(9));
            writer.create(schema, out);

            for(int i = 0; i < 1_000_000; i++){
                GenericRecord record = new GenericData.Record(schema);
                record.put("user", "user" + i % 7);
                record.put("pwd", "pwd" + i % 7);
                writer.append(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
