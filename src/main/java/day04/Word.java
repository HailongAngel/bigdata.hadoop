package day04;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Word {
public static void main(String[] args) throws Exception, InterruptedException, URISyntaxException {
	FileSystem fs =FileSystem.get(new URI("hdfs://hadoop01:9000"),new Configuration(),"root");
	FSDataInputStream open = fs.open(new Path("/a.txt"));
	BufferedReader br = new BufferedReader(new InputStreamReader(open,"utf-8"));
	BufferedWriter bw = new BufferedWriter(new FileWriter("e:/upload.txt"));
	String str = null;
	while((str = br.readLine())!=null) {
		bw.write(str);
	}
	bw.close();
    br.close();
	
}
}
