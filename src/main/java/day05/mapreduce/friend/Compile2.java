package day05.mapreduce.friend;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import day05.mapreduce.friend.Compile2.MapTask.ReduceTask;

 class Compile2 {
	 public static class MapTask extends Mapper<LongWritable, Text,Text, Text>{
		 @Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new Text(split[0]), new Text(split[1]));
	  }
	  public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
		  @Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			  String friends="";
			for (Text text : values) {
				friends += text+" ";
			}
		  context.write(new Text(key),new Text(friends));
				
			}
			
			  
		}
	  }
	  public static void main(String[] args) {
		  try {
				
				Configuration conf = new Configuration();
				
				Job job = Job.getInstance(conf);
				job.setMapperClass(MapTask.class);
				job.setReducerClass(ReduceTask.class);
				job.setJarByClass(Compile2.class);
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				FileSystem fs = FileSystem.get(conf);
				if(fs.exists(new Path("d:\\data\\out\\friend1"))) {
					fs.delete(new Path("d:\\data\\out\\friend1"),true);
				}
						
				FileInputFormat.addInputPath(job, new Path("d:\\data\\out\\friend\\part-r-00000"));
				FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\friend1"));
				
				boolean completion = job.waitForCompletion(true);
				System.out.println(completion?"³É¹¦":"Ê§°Ü");
			}
			catch(Exception e) {
				
			}
			}
}
