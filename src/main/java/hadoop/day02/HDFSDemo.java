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
 * javaApi����HDFS
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
	    * �ϴ��ļ�
	    * @throws Exception
	    * @throws IOException
	    */
	   @Test
	   public void testUpLoad() throws Exception, IOException {
		   fs.copyFromLocalFile(new Path("C:\\Users\\Hailong\\Pictures\\Camera Roll\\wife.jpg"),new Path("/wife.jpg"));
	   }
	   /**
	    * �����ļ�
	    * ��ҪHADOOP_HOME   �� PATH
	    * ����Ҫ��ѹһ��windows����õİ������Ƶ�hadoop_home�����binĿ¼ȥ
	    * @throws Exception
	    * @throws IOException
	    */
	   @Test
	   public void testDownLoad() throws Exception, IOException {
		   fs.copyToLocalFile(new Path("/wife.jpg"), new Path("d:\\wife.jpg"));
	   }
	   /**
	    * ɾ���ļ�
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	    */
	   @Test
	   public void testDelete() throws IllegalArgumentException, IOException {
		   fs.delete(new Path("/aa"), true);
	   }
	   /**
	    * ����Ŀ¼
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	    */
	   @Test
	   public void testmkdir() throws IllegalArgumentException, IOException {
		   fs.mkdirs(new Path("/mkdit"));
	   }
	   /**
	    * �������ƶ�
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	    */
	   @Test
	   public void testmv() throws IllegalArgumentException, IOException {
		 //�ƶ���ʱ����ҪĿ¼���ڣ������ڵ�ʱ�����쳣������ִ�в��ɹ�
		   fs.rename(new Path("/mkdir"), new Path("/data/mkdir"));
	   }
	   
	   /**
	    * �ļ���״̬
	    * �г����Ķ����ļ�
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	    */
	   @Test
	   public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		   RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		   while(listFiles.hasNext()) {
			   //�����ļ�
			   LocatedFileStatus fileInfo = listFiles.next();
	
			   System.out.println(fileInfo.getAccessTime());//�õ�����޸�ʱ��
			   System.out.println(fileInfo.getLen());//�õ��ļ�����
			   System.out.println(fileInfo.getBlockSize()); //���С
			   System.out.println(fileInfo.getPath()); //�õ��ļ�·��
			   System.out.println(fileInfo.getReplication()); //��������
			   System.out.println("---------------------------");
			   //��ȡ�����ļ���  �ļ����Ӧ��ƫ������Ϣ  ���������
			   BlockLocation[] blockLocations = fileInfo.getBlockLocations();
			   System.out.println(blockLocations);
		   }
		   System.out.println("+++++++++++++++++++++++");
	   }
	   /**
	    * �г��ļ������������ļ��Լ�Ŀ¼
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
				   System.out.println("����һ���ļ���");
			   }
			   if(fileStatus.isFile()) {
				   System.out.println("����һ���ļ�");
			   }
			   System.out.println(fileStatus.getAccessTime());//�õ�����޸�ʱ��
			   System.out.println(fileStatus.getLen());//�õ��ļ�����
			   System.out.println(fileStatus.getBlockSize()); //���С
			   System.out.println(fileStatus.getPath()); //�õ��ļ�·��
			   System.out.println(fileStatus.getReplication()); //��������
			   
			
			   System.out.println("--------------------------");
		}
	   }
	   @After
	   public void close() throws IOException {
		   fs.close();
	   }
}
