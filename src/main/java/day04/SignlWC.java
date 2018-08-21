package day04;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SignlWC {
  public static void main(String[] args) {
	  try {
		  Map<String,Integer> map = new HashMap<>(); 
	 Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root");
	FSDataInputStream open = fs.open(new Path("/wc.sh"));
	BufferedReader br = new BufferedReader(new InputStreamReader(open,"utf-8"));
	String str = null;
	while((str = br.readLine())!=null) {
		String[] split = str.split(" ");
		for (String word : split) {
			Integer count = map.getOrDefault(word, 0);
			count++;
			map.put(word, count);
		}
	}
	Set<Entry<String, Integer>> entrySet = map.entrySet();
	for (Entry<String, Integer> entry : entrySet) {
		System.out.println(entry);
		
	}
	  }
	  catch(Exception e) {
		  e.printStackTrace();
	  }
	
}
}
