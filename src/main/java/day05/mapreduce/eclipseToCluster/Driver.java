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
		// 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
			System.setProperty("HADOOP_USER_NAME", "root");
			
			
			Configuration conf = new Configuration();
			// 1、设置job运行时要访问的默认文件系统
			conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
			// 2、设置job提交到哪去运行
			conf.set("mapreduce.framework.name", "yarn");
			conf.set("yarn.resourcemanager.hostname", "hadoop01");
			// 3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
			conf.set("mapreduce.app-submission.cross-platform","true");
		   
		   
		   Job job = Job.getInstance(conf);
		 // 1、封装参数：jar包所在的位置
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
		   
		    // 5、封装参数：想要启动的reduce task的数量
			job.setNumReduceTasks(2);
		   
		   boolean completion = job.waitForCompletion(true);
			System.out.println(completion?"程序执行完毕，没毛病！！！":"程序有问题，程序出bug了，赶紧加班调试！！！");
	   }
	   catch(Exception e) {
		   
	   }
}
}
