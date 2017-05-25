package com.hadoop.test.hdfs.impl;

import com.hadoop.test.HadoopAvroTestApplication;
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
import org.apache.hadoop.util.Progressable;

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
        String dest = "hdfs://localhost/user/yshi/avro_test";
        Configuration conf = new Configuration();
        try(FileSystem fs = FileSystem.get(URI.create(dest), conf);
            OutputStream out = fs.create(new Path(dest), () -> System.out.print("."));
            DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<Object>())){

            Schema schema = new Schema.Parser().parse(SCHEMA);
            writer.create(schema, out);
            //writer.setCodec(CodecFactory.snappyCodec());

            for(int i = 0; i < 2000000; i++){
                GenericRecord record = new GenericData.Record(schema);
                record.put("user", "user" + i % 4);
                record.put("pwd", "pwd" + i % 4);
                writer.append(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
