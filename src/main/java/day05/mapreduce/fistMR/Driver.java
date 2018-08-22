package day05.mapreduce.fistMR;


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
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); //加载集群上的配置文件	
		//conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		Job job = Job.getInstance(conf);
		
		//设置job的map和reduce是哪一个，并且设置是哪一做任务提交
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		job.setJarByClass(Driver.class);
		
		//设置输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path("/wc.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/wc-output"));
		
		//查看提交之前是否存在
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("/wordcount/wc-output"))){
			fs.delete(new Path("/wordcount/wc-output"),true);
		}
		
		// 提交之后会监控运行状态
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"程序执行完毕，没毛病！！！":"程序有问题，程序出bug了，赶紧加班调试！！！");
		
	}
}
