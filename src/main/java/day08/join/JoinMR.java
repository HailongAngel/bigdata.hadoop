package day08.join;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 将两个不同文件的数据进行拼接
 * 有相同的Uid
 * @author Hailong
 *思路：map阶段读取问价内容，但是要记下文件的名称，方便之后的对接
 *reduce阶段分别读取文件内容
 *将rating文件的内容追加到User问价的用户后面
 */

public class JoinMR {
	public static class MapTask extends Mapper<LongWritable, Text, Text, JoinBean>{
		String table;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			table = fileSplit.getPath().getName();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			
				
				String[] split = value.toString().split("::");
				JoinBean joinBean = new JoinBean();
				System.out.println(split.length);
				if(split.length>=3) {
				if(table.startsWith("users")){
					//1::F::1::10::48067
				
					joinBean.set(split[0], split[2], split[1],"null", "null","user");
				}else{
					//1::1193::5::978300760
					joinBean.set(split[0], "null", "null", split[1], split[2], "rating");
				}
				context.write(new Text(joinBean.getUid()), joinBean);
				}
				
			
			
			}
	}
	
	public static class ReduceTask extends Reducer<Text, JoinBean, JoinBean, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<JoinBean> values,
				Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {
			//放user的
			JoinBean joinBean = new JoinBean();
			//rating
			ArrayList<JoinBean> list = new ArrayList<>();
			
			//分离数据
			for (JoinBean joinBean2 : values) {
				String table = joinBean2.getTable();
				if("user".equals(table)){
					joinBean.set(joinBean2.getUid(), joinBean2.getAge(), joinBean2.getGender(), joinBean2.getMovieId(), joinBean2.getRating(), joinBean2.getTable());
				}else{
					JoinBean joinBean3 = new JoinBean();
					joinBean3.set(joinBean2.getUid(), joinBean2.getAge(), joinBean2.getGender(), joinBean2.getMovieId(), joinBean2.getRating(), joinBean2.getTable());
					list.add(joinBean3);
				}
			}
			
			//拼接数据
			for (JoinBean joinBean2 : list) {
				joinBean2.setAge(joinBean.getAge());
				joinBean2.setGender(joinBean.getGender());
				context.write(joinBean2, NullWritable.get());
			}
		}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "join");
		
		//设置map和reduce，以及提交的jar
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setJarByClass(JoinMR.class);
		
		//设置输入输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinBean.class);
		
		job.setOutputKeyClass(JoinBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//输入和输出目录
		FileInputFormat.addInputPath(job, new Path("d:/data/in/join"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\join"));
		
		//判断文件是否存在
		File file = new File("d:\\data\\out\\join");
		if(file.exists()){
			FileUtils.deleteDirectory(file);
		}
		
		//提交任务
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"你很优秀！！！":"滚去调bug！！");
		
	}
	}
}
