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
 * ��ͬ����
 * @author Hailong
 * 1.��Ϊÿ���û��Ĺ�ͬ��������mapreduce����ʵ�֣����Է���������ѵ��û�
 * ���������������������Щ�û���������������û������û��Ĺ�ͬ����
 * B-C	A
   B-D	A
   B-F	A
   B-G	A
   B-H	A
 * 2.�����ͬ����
 * ͨ����һ��mapreduce����֮���������û�Ϊkey�����value
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
		  Collections.sort(userList); //��Ҫ��������Ϊ����A-B/B-Akeyֵ����ͬ��������������ǰ���������OK
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
			System.out.println(completion?"�ɹ�":"ʧ��");
		}
		catch(Exception e) {
			
		}
		}
	

}
