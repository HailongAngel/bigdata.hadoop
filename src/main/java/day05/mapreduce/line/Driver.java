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
			conf.set("fs.defaultFS", "hdfs://hadoop01:9000"); //����hdfs��Ⱥ������
			conf.set("mapreduce.framework.name", "yarn"); //�ύ������   yarn   local
			conf.set("yarn.resourcemanager.hostname", "hadoop01");  //resourcemeanger ������
			conf.set("mapreduce.app-submission.cross-platform", "true"); //windows �ύ����linux����Ҫ���õĲ���
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
		System.out.println(completion?"����ִ����ϣ�ûë��������":"���������⣬�����bug�ˣ��Ͻ��Ӱ���ԣ�����");
	  }
	  catch(Exception e) {
		  
	  }
}
}
