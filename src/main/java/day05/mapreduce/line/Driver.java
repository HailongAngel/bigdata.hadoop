package day05.mapreduce.line;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
  public static void main(String[] args) {
	  try {
		  System.setProperty("HADOOP_USER_NAME", "root");
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://hadoop01:9000"); //设置hdfs集群在哪里
			conf.set("mapreduce.framework.name", "yarn"); //提交到哪里   yarn   local
			conf.set("yarn.resourcemanager.hostname", "hadoop01");  //resourcemeanger 在哪里
			conf.set("mapreduce.app-submission.cross-platform", "true"); //windows 提交任务到linux上需要设置的参数
		  Job job = Job.getInstance(conf,"ex");
		  
		  job.setMapperClass(LineCoincide.class);
		  job.setReducerClass(LinecountReducer.class);
		  //job.setJarByClass(Driver.class);
		  job.setJar("C:\\Users\\Hailong\\Desktop\\w2.jar"); 
		  job.setMapOutputKeyClass(IntWritable.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(IntWritable.class);
		  
		  
		  FileInputFormat.addInputPath(job, new Path("/line.txt"));
		  FileOutputFormat.setOutputPath(job,new Path("/line/count/"));
		  FileSystem fs = FileSystem.get(conf);
		  if(!fs.exists(new Path("/line/count/"))) {
			  fs.delete(new Path("/line/count/"),true);
			  
		  }
		  
	 boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"程序执行完毕，没毛病！！！":"程序有问题，程序出bug了，赶紧加班调试！！！");
	  }
	  catch(Exception e) {
		  
	  }
}
}
