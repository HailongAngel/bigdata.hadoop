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

		// ����map��reduce���Լ��ύ��jar
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setJarByClass(Avg.class);

		// ���������������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// �ж��ļ��Ƿ����
		File file = new File("d:\\data\\out\\movie");
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		// ��������Ŀ¼
		FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\movie"));

		// �ύ����
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion ? "������㣡����" : "��ȥ��bug����");

	} catch (Exception e) {
		e.printStackTrace();
		// TODO: handle exception
	}

}
}
