package com.hadoop.test.hdfs.impl;

import com.hadoop.test.hdfs.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FileSystemReader implements Reader {

    public void read(){
        System.out.println("=================== File System ===================");
        String uri = "hdfs://localhost:8020/user/yshi/test";
        Configuration conf = new Configuration();
        try(FileSystem fs = FileSystem.get(URI.create(uri), conf);
            InputStream in = fs.open(new Path(uri))){
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
