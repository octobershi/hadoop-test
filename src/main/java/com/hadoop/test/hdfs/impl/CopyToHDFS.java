package com.hadoop.test.hdfs.impl;

import com.hadoop.test.hdfs.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class CopyToHDFS implements Writer {

    @Override
    public void write() {
        String local = "/usr/local/opt/hadoop-2.8.0/bin/test";
        String dest = "hdfs://localhost/user/yshi/new_test";
        Configuration conf = new Configuration();
        try(InputStream in = new BufferedInputStream(new FileInputStream(local));
            FileSystem fs = FileSystem.get(URI.create(dest), conf);
            OutputStream out = fs.create(new Path(dest), () -> System.out.print("."))){
            IOUtils.copyBytes(in, out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
