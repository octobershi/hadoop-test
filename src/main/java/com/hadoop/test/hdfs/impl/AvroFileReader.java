package com.hadoop.test.hdfs.impl;

import com.hadoop.test.hdfs.Reader;
import com.hadoop.test.hdfs.Writer;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class AvroFileReader implements Reader {


    @Override
    public void read() {
        String uri = "hdfs://localhost:8020/user/yshi/avro_test_output/part-r-00000.avro";
        Configuration conf = new Configuration();
        try(FileSystem fs = FileSystem.get(URI.create(uri), conf);
            InputStream in = fs.open(new Path(uri));
            DataFileStream<Object> is = new DataFileStream<>(in, new GenericDatumReader<>())){

            for(Object o : is){
                GenericRecord record = (GenericRecord)o;
                System.out.println(record.get("key")+ "\t:"+record.get("value"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
