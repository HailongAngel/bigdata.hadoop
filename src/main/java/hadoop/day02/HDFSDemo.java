package hadoop.day02;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * 
 * javaApi操作HDFS
 * @author Hailong
 *
 */
public class HDFSDemo {
  
	   FileSystem fs = null;
	   @Before
	   public void init() throws IOException, InterruptedException, URISyntaxException {
		   Configuration conf = new Configuration();
		   fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root");
	   
   }
	   /**
	    * 上传文件
	    * @throws Exception
	    * @throws IOException
	    */
	   @Test
	   public void testUpLoad() throws Exception, IOException {
		   fs.copyFromLocalFile(new Path("C:\\Users\\Hailong\\Pictures\\Camera Roll\\wife.jpg"),new Path("/wife.jpg"));
	   }
	   /**
	    * 下载文件
	    * 需要HADOOP_HOME   和 PATH
	    * 还需要解压一个windows编译好的包，复制到hadoop_home下面的bin目录去
	    * @throws Exception
	    * @throws IOException
	    */
	   @Test
	   public void testDownLoad() throws Exception, IOException {
		   fs.copyToLocalFile(new Path("/wife.jpg"), new Path("d:\\wife.jpg"));
	   }
	   /**
	    * 删除文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	    */
	   @Test
	   public void testDelete() throws IllegalArgumentException, IOException {
		   fs.delete(new Path("/aa"), true);
	   }
	   /**
	    * 创建目录
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	    */
	   @Test
	   public void testmkdir() throws IllegalArgumentException, IOException {
		   fs.mkdirs(new Path("/mkdit"));
	   }
	   /**
	    * 改名、移动
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	    */
	   @Test
	   public void testmv() throws IllegalArgumentException, IOException {
		 //移动的时候需要目录存在，不存在的时候不抛异常，但是执行不成功
		   fs.rename(new Path("/mkdir"), new Path("/data/mkdir"));
	   }
	   
	   /**
	    * 文件的状态
	    * 列出来的都是文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	    */
	   @Test
	   public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		   RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		   while(listFiles.hasNext()) {
			   //单个文件
			   LocatedFileStatus fileInfo = listFiles.next();
	
			   System.out.println(fileInfo.getAccessTime());//得到最后修改时间
			   System.out.println(fileInfo.getLen());//得到文件长度
			   System.out.println(fileInfo.getBlockSize()); //块大小
			   System.out.println(fileInfo.getPath()); //得到文件路径
			   System.out.println(fileInfo.getReplication()); //副本数量
			   System.out.println("---------------------------");
			   //获取所有文件快  文件快对应的偏移量信息  存放在哪里
			   BlockLocation[] blockLocations = fileInfo.getBlockLocations();
			   System.out.println(blockLocations);
		   }
		   System.out.println("+++++++++++++++++++++++");
	   }
	   /**
	    * 列出文件夹下面所有文件以及目录
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	    * 
	    */
	   @Test
	   public void listStatus() throws FileNotFoundException, IllegalArgumentException, IOException {
		   FileStatus[] status = fs.listStatus(new Path("/"));
		   for (FileStatus fileStatus : status) {
			   if(fileStatus.isDirectory()) {
				   System.out.println("这是一个文件夹");
			   }
			   if(fileStatus.isFile()) {
				   System.out.println("这是一个文件");
			   }
			   System.out.println(fileStatus.getAccessTime());//得到最后修改时间
			   System.out.println(fileStatus.getLen());//得到文件长度
			   System.out.println(fileStatus.getBlockSize()); //块大小
			   System.out.println(fileStatus.getPath()); //得到文件路径
			   System.out.println(fileStatus.getReplication()); //副本数量
			   
			
			   System.out.println("--------------------------");
		}
	   }
	   @After
	   public void close() throws IOException {
		   fs.close();
	   }
}
