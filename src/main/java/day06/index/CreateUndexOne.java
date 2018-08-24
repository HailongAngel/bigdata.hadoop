package day06.index;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class CreateUndexOne {
	//  hello  hello hadoop  --------> hello-a.txt  1
	public static class MapTask extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String name = fileSplit.getPath().getName();
			String[] split = value.toString().split(" ");
			for (String string : split) {
				context.write(new Text(string + "-" +name), new IntWritable(1));
			}
		}
	}
	
	public static class ReduceTask extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable intWritable : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
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
			job.setJarByClass(CreateUndexOne.class);

			// ���������������
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			// �ж��ļ��Ƿ����
			File file = new File("d:\\data\\out\\indexOne");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			// ��������Ŀ¼
			FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\index"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\indexOne"));

			// �ύ����
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion ? "������㣡����" : "��ȥ��bug����");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}
        

}
