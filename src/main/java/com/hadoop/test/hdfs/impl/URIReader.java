package com.hadoop.test.hdfs.impl;

import com.hadoop.test.hdfs.Reader;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class URIReader implements Reader {

    static {
        System.setProperty("hadoop.home.dir", "/usr/local/opt/hadoop-2.8.0/");
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public void read(){
        System.out.println("=================== URI ===================");
        try(InputStream is = new URL("hdfs://localhost:8020/user/yshi/test").openStream()){
            IOUtils.copyBytes(is, System.out, 4096, false);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
