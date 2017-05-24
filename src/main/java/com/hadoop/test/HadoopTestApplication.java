package com.hadoop.test;

import com.hadoop.test.hdfs.Reader;
import com.hadoop.test.hdfs.Writer;
import com.hadoop.test.hdfs.impl.CopyToHDFS;
import com.hadoop.test.hdfs.impl.FileSystemReader;
import com.hadoop.test.hdfs.impl.URIReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HadoopTestApplication {

	public static void main(String[] args) {

		SpringApplication.run(HadoopTestApplication.class, args);
//		Reader reader = new URIReader();
//		reader.read();

		Writer writer = new CopyToHDFS();
		writer.write();

	}
}
