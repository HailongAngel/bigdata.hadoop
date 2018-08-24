package day06.movie;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;


public class Avg {
public static class MapTask extends Mapper<LongWritable, Text, Text, IntWritable>	{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		ObjectMapper objectMapper = new ObjectMapper();
		MovieBean bean = objectMapper.readValue(value.toString(), MovieBean.class);
		context.write(new Text(bean.getMovie()), new IntWritable(bean.getRate()));
	}
}

public static class ReduceTask extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		for (IntWritable intWritable : values) {
			sum += intWritable.get();
			count++;
		}
		context.write(new Text(key), new IntWritable(sum/count));
		
	}
}


public static void main(String[] args) {
	try {
		//System.setProperty("HADOOP_USER_NAME", "SIMPLE");
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		// 设置map和reduce，以及提交的jar
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setJarByClass(Avg.class);

		// 设置输入输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 判断文件是否存在
		File file = new File("d:\\data\\out\\movie");
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		// 输入和输出目录
		FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\movie"));

		// 提交任务
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion ? "你很优秀！！！" : "滚去调bug！！");

	} catch (Exception e) {
		e.printStackTrace();
		// TODO: handle exception
	}

}
}
