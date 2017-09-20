package com.hadoop.demo.hdfs.impl;

import com.hadoop.demo.hdfs.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class CommonFileWriter implements Writer {

    @Override
    public void write() {
        //System.setProperty("HADOOP_USER_NAME", "yshi");
        String dest = "hdfs://localhost:8020/user/yshi/common_test_big";
        Configuration conf = new Configuration();
        try(FileSystem fs = FileSystem.get(URI.create(dest), conf);
            OutputStream out = fs.create(new Path(dest), () -> System.out.print("."))){

            for(int i = 0; i < 100_000_000; i++){
                out.write(("user" + i % 4 + "\n").getBytes());
                out.write(("pwd" + i % 4 + "\n").getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
