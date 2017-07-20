package com.hadoop.demo;

import com.hadoop.demo.hdfs.Reader;
import com.hadoop.demo.hdfs.Writer;
import com.hadoop.demo.hdfs.impl.AvroFileReader;
import com.hadoop.demo.hdfs.impl.AvroFileWriter;
import com.hadoop.demo.hdfs.impl.CommonFileWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopTestApplicationTests {

    private Writer avroWriter;
    private Writer commonWriter;
    private Reader reader;

    public HadoopTestApplicationTests() {
    }

    @Before
    public void init() {
        reader = new AvroFileReader();
        avroWriter = new AvroFileWriter();
        commonWriter = new CommonFileWriter();
    }

    @Test
    public void writeAvroContent() {
        avroWriter.write();
    }

    @Test
    public void writeCommonContent() {
        commonWriter.write();
    }


}
