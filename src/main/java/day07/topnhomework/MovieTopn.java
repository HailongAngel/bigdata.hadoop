package day07.topnhomework;

import java.io.File;
import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * 求出每个用户观看电影的前20个评分高的电影
 * @author Hailong
 *
 */
public class MovieTopn {
  public static class MapTask extends Mapper<LongWritable, Text, MovieBean, NullWritable>{
	  @Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, MovieBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		    ObjectMapper objectMapper = new ObjectMapper();
		    MovieBean readValue = objectMapper.readValue(value.toString(), MovieBean.class);
		    context.write(readValue, NullWritable.get());
	}
  }
  
  public static class ReduceTask extends Reducer<MovieBean, NullWritable, MovieBean, NullWritable>{
	  @Override
	protected void reduce(MovieBean movieBean, Iterable<NullWritable> values,
			Reducer<MovieBean, NullWritable, MovieBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		int  count = 0;
		for (NullWritable nullWritable : values) {
			if(count>=20) {
				break;
			}
			else {
				count++;
				context.write(movieBean, NullWritable.get());
			}
		}
	}
  }
  
  
  public static void main(String[] args) {
	  try {
		  
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance();
		  
		  job.setMapperClass(MapTask.class);
		  job.setReducerClass(ReduceTask.class);
		  job.setJarByClass(MovieBean.class);
		  job.setNumReduceTasks(2);
		  job.setGroupingComparatorClass(MyGroup.class);
		  job.setPartitionerClass(MyPartitition.class);
		  
		  
		  job.setMapOutputKeyClass(MovieBean.class);
		  job.setMapOutputValueClass(NullWritable.class);
		  job.setOutputKeyClass(MovieBean.class);
		  job.setOutputValueClass(NullWritable.class);
		  
		  
		  FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
		  FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\homework"));
		  
		  File file = new File("d:\\data\\out\\homework");
	       if(file.exists()) {
	    	   FileUtils.deleteDirectory(file);
	       }
	       
	       boolean completion = job.waitForCompletion(true);
	       System.out.println(completion?"执行成功":"滚去调代码");
	       
	  }
       catch(Exception e) {
    	   
       }
}
}
