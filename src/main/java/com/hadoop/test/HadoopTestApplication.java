package com.hadoop.test;

import com.hadoop.test.hdfs.Reader;
import com.hadoop.test.hdfs.Writer;
import com.hadoop.test.hdfs.impl.AvroFileReader;
import com.hadoop.test.hdfs.impl.AvroFileWriter;

public class HadoopTestApplication {

	public static void main(String[] args) {

//		Reader reader = new AvroFileReader();
//		reader.read();

		Writer writer = new AvroFileWriter();
		writer.write();

	}
}
