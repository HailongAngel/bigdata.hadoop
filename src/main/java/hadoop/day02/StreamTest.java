package hadoop.day02;
/**
 * 流数据读取和写入文件
 * @author Hailong
 *
 */

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Options.FSDataInputStreamOption;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamTest {
	FileSystem fs = null;

	@Before
	public void init() throws IOException {
		System.setProperty("HADOOP_USER_NAME", "root");
		// 加载配置信息 能够加载core-site.xml hdfs-site.xml mapred-site.xml yarn...
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		fs = FileSystem.get(conf);
	}

	/**
	 * 流文件写入
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void writeFile() throws IllegalArgumentException, IOException {
		FSDataOutputStream output = fs.create(new Path("/writeFile.txt"));
		output.write(1);
		output.writeUTF("hello");
		output.write("hello".getBytes());
	}
	
	/**
	 * 读取流文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void readFile() throws IllegalArgumentException, IOException {
		FSDataInputStream input = fs.open(new Path("/writeFile.txt"));
		int read = input.read();
		String readUTF = input.readUTF();
		System.out.println(read);
		System.out.println(readUTF);
	}
	
	/**
	 * 流文件上传
	 * @throws IOException
	 */
	@Test
	public void uploadFile() throws IOException {
    FSDataOutputStream output = fs.create(new Path("/upload.txt"));
	BufferedReader br = new BufferedReader(new FileReader("D:\\upload.txt"));
	String str = null;
	 while((str=br.readLine())!=null) {
		output.write(str.getBytes());
	}
	}
	
	/**
	 * 流文件下载
	 * @throws IllegalArgumentException 
	 * @throws IOException
	 */
	@Test
	public void downLoad() throws IllegalArgumentException, IOException {
		FSDataInputStream input = fs.open(new Path("/upload.txt"));
		BufferedReader  br = new BufferedReader(new InputStreamReader(input,"utf-8"));
		 BufferedWriter bw = new BufferedWriter(new FileWriter("e:/upload.txt"));
		 String str = null;
		 while((str = br.readLine())!= null) {
			 bw.write(str);
		 }
		 bw.close();
		 br.close();
		/* FileOutputStream fos = new FileOutputStream("e:/download.txt");
		 byte[] b = new byte[1024];
		 int len;
		 while((len = input.read(b))!=-1) {
		  fos.write(b,0,len);
		  }
		 fos.close();
		 input.close();*/
		
	
	}
	@After
	public void close() throws IOException {
		fs.close();
	}
	
}
