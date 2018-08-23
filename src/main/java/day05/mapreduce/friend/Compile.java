package day05.mapreduce.friend;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * 求共同好友
 * @author Hailong
 * 1.因为每个用户的共同好友利用mapreduce不好实现，所以反过来求好友的用户
 * 这样就能求出来好友有哪些用户，两两组合起来用户就是用户的共同好友
 * B-C	A
   B-D	A
   B-F	A
   B-G	A
   B-H	A
 * 2.求出共同好友
 * 通过第一个mapreduce处理，之后以两个用户为key，求出value
 *
 */

public class Compile {
  public static class MapTask extends Mapper<LongWritable, Text,Text, Text>{
	  @Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] user = value.toString().split(":");
		String[] friend = user[1].split(",");
			for (String string1 : friend) {
				context.write(new Text(string1),new Text(user[0]) );
				
			}
		}
  }
  public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
	  @Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		 /* for (Text friend1 : values) {
			   for (Text friend2 : values) {
				   if(!friend1.equals(friend2)) {
					   context.write(new Text(friend1+"-"+friend2), key);
					   System.out.println(friend1+"-"+friend2);
				   }
			}
		}*/
		  List<String> userList = new ArrayList<>();
		  for (Text string : values) {
			  userList.add(string.toString());
			
		}
		  Collections.sort(userList); //需要排序是因为避免A-B/B-Akey值不相同的情况，排完序从前往后遍历就OK
		  for(int i=0;i<userList.size()-1;i++) {
			  for(int j=i+1;j<userList.size();j++) {
				  context.write(new Text(userList.get(i)+"-"+userList.get(j)), key);
			  }
		  }
		  
	}
  }
  public static void main(String[] args) {
	  try {
			
			Configuration conf = new Configuration();
			
			Job job = Job.getInstance(conf);
			job.setMapperClass(MapTask.class);
			job.setReducerClass(ReduceTask.class);
			job.setJarByClass(Compile.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path("d:\\data\\out\\friend"))) {
				fs.delete(new Path("d:\\data\\out\\friend"),true);
			}
					
			FileInputFormat.addInputPath(job, new Path("e:\\data\\friend.txt"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\friend"));
			
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion?"成功":"失败");
		}
		catch(Exception e) {
			
		}
		}
	

}
