package day05.mapreduce.distionct;

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

/**
 * 手机号前三位归属地
 * 
 * @author Hailong
 *
 */
public class Dis {
	public static class MapTask extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			if (key.get() != 0L) {
				for (String string : split) {
					context.write(new Text(split[0]), new Text(split[4]));
				}
			}
		}
	}

	public static class ReduceTask extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(key, text);
				break;
				
			}

		}
	}
	
	public static void main(String[] args) {
		try {
			
			Configuration conf = new Configuration();
			
			Job job = Job.getInstance(conf);
			job.setMapperClass(MapTask.class);
			job.setReducerClass(ReduceTask.class);
			job.setJarByClass(Dis.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path("d:\\data\\out\\phone"))) {
				fs.delete(new Path("d:\\data\\out\\phone"),true);
			}
					
			FileInputFormat.addInputPath(job, new Path("e:\\data\\phone.txt"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\phone"));
			
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion?"成功":"失败");
		}
		catch(Exception e) {
			
		}
		}
	
}
