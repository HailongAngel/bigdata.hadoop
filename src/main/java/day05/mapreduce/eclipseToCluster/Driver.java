package day05.mapreduce.eclipseToCluster;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
   public static void main(String[] args) {
	   try {
		// �ڴ���������JVMϵͳ���������ڸ�job��������ȡ����HDFS���û����
			System.setProperty("HADOOP_USER_NAME", "root");
			
			
			Configuration conf = new Configuration();
			// 1������job����ʱҪ���ʵ�Ĭ���ļ�ϵͳ
			conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
			// 2������job�ύ����ȥ����
			conf.set("mapreduce.framework.name", "yarn");
			conf.set("yarn.resourcemanager.hostname", "hadoop01");
			// 3�����Ҫ��windowsϵͳ���������job�ύ�ͻ��˳�������Ҫ�������ƽ̨�ύ�Ĳ���
			conf.set("mapreduce.app-submission.cross-platform","true");
		   
		   
		   Job job = Job.getInstance(conf);
		 // 1����װ������jar�����ڵ�λ��
		   job.setJar("C:\\Users\\Hailong\\Desktop\\w2.jar"); 
		   job.setMapperClass(WordcountMapper.class);
		   job.setReducerClass(WordcountReducer.class);
		   
		   job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(IntWritable.class);
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(IntWritable.class);
		   
		   FileSystem fs = FileSystem.get(conf);
		   if(!fs.exists(new Path("/wordcount/eclipse-out/"))) {
			  fs.delete(new Path("/wordcount/eclipse-out/"),true);
		   }
		   
		   FileInputFormat.addInputPath(job, new Path("/wc.txt"));
		   FileOutputFormat.setOutputPath(job, new Path("/wordcount/eclipse-out/"));
		   
		    // 5����װ��������Ҫ������reduce task������
			job.setNumReduceTasks(2);
		   
		   boolean completion = job.waitForCompletion(true);
			System.out.println(completion?"����ִ����ϣ�ûë��������":"���������⣬�����bug�ˣ��Ͻ��Ӱ���ԣ�����");
	   }
	   catch(Exception e) {
		   
	   }
}
}
