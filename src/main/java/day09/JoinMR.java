package day09;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 小表套大表可以节省时间
 * 在map阶段就可以实现
 * 思路：
 * 在setup阶段将小表分隔，将key找出来，在map阶段根据key从而找到大文件中
 * 自己所对应的数据，将数据放入对象，从而实现拼接
 * 线索是key
 */

public class JoinMR {
	public static class MapTask extends Mapper<LongWritable, Text, JoinBean, NullWritable>{
		Map<String,String> map = new HashMap<>();
		@Override
		protected void setup(Context context)  
				throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String smallTableName = conf.get("smallTableName");
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream open = fs.open(new Path(smallTableName));
		BufferedReader br = new BufferedReader(new InputStreamReader(open));
		String line = null;
		while((line = br.readLine())!=null) {
			String[] split = line.split("::");
			map.put(split[0], line);
		}
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("::");
			JoinBean joinBean = new JoinBean();
			String[] line = map.get(split[0]).split("::");
			joinBean.set(split[0], line[2], line[1], split[1], split[2], "null");
			context.write(joinBean,NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{

		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		// 1、设置job运行时要访问的默认文件系统
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		// 2、设置job提交到哪去运行
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "hadoop01");
		// 3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
		conf.set("mapreduce.app-submission.cross-platform","true");
	    conf.set("smallTableName", args[2]);
	   
	   Job job = Job.getInstance(conf);
	 // 1、封装参数：jar包所在的位置
	   job.setJar("C:\\Users\\Hailong\\Desktop\\w2.jar"); 
	   job.setMapperClass(MapTask.class);
	   job.setOutputKeyClass(JoinBean.class);
	   job.setOutputValueClass(NullWritable.class);
	   
	   FileSystem fs = FileSystem.get(conf);
	   if(!fs.exists(new Path("/data/out/join"))) {
		  fs.delete(new Path("/data/out/join"),true);
	   }
	   
	   FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	    // 5、封装参数：想要启动的reduce task的数量
		job.setNumReduceTasks(2);
	   
	   boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"程序执行完毕，没毛病！！！":"程序有问题，程序出bug了，赶紧加班调试！！！");
   }
   

	
	
   
}
