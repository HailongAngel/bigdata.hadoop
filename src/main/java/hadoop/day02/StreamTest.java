package hadoop.day02;
/**
 * �����ݶ�ȡ��д���ļ�
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
		// ����������Ϣ �ܹ�����core-site.xml hdfs-site.xml mapred-site.xml yarn...
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		fs = FileSystem.get(conf);
	}

	/**
	 * ���ļ�д��
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
	 * ��ȡ���ļ�
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
	 * ���ļ��ϴ�
	 * @throws IOException
	 */
	@Test
	public void uploadFile() throws IOException {
    FSDataOutputStream output = fs.create(new Path("/a.txt"));
	BufferedReader br = new BufferedReader(new FileReader("D:\\a.txt"));
	String str = null;
	 while((str=br.readLine())!=null) {
		output.write(str.getBytes());
	}
	}
	
	/**
	 * ���ļ�����
	 * @throws IllegalArgumentException 
	 * @throws IOException
	 */
	@Test
	public void downLoad() throws IllegalArgumentException, IOException {
		FSDataInputStream input = fs.open(new Path("/a.txt"));
		 BufferedWriter bw   = new BufferedWriter(new FileWriter("E:\\download.txt"));
		 byte[] b = new byte[1024];
		 int len;
		 while((len = input.read(b))!=-1) {
		  bw.write(b.toString());
		 /*
		 BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File("E:/m.txt")));
		 byte[] b = new byte[1024];
		 int len;
		 while((len=input.read(b))!=-1) {
			 System.out.println(len);
			 bos.write(b,0,len);
		 }*/
	}
		 bw.close();
	}
	@After
	public void close() throws IOException {
		fs.close();
	}
	
}
