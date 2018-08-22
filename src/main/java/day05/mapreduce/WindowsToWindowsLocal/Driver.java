package day05.mapreduce.WindowsToWindowsLocal;



import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		//声明使用哪个用户提交的
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		
		/**
		 * fs.defaultFS   默认值 file:///    本地文件系统
		 * mapreduce.framework.name   默认值  local
		 */
		Job job = Job.getInstance(conf, "WindowsToWindowsLocal");
		
		//设置map和reduce，以及提交的jar
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		job.setJarByClass(Driver.class);
		
		//设置输入输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//输入和输出目录
		FileInputFormat.addInputPath(job, new Path("E:\\data\\word.txt"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\wc"));
		
		//判断文件是否存在
		File file = new File("d:\\data\\out\\wc");
		if(file.exists()){
			FileUtils.deleteDirectory(file);
		}
		
		//提交任务
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?0:1);
		
	}
}
