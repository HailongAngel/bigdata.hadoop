package day05.mapreduce.line;

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

public class LineMr {
	/**
	 * �غϵ�ļ���
	 */

	public static class MapTask extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				String[] split = value.toString().split(",");
				int start = Integer.parseInt(split[0]);
				int end = Integer.parseInt(split[1]);
				for(int i = start; i<=end ; i++){
					context.write(new Text(i+""), new IntWritable(1));
				}
			} catch (Exception e) {
				
			}
		}
	}
	
	public static class ReduceTask extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
			int count = 0 ;
			for (IntWritable intWritable : arg1) {
				count +=intWritable.get();
			}
			arg2.write(arg0, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
				Configuration conf = new Configuration();
				
				Job job = Job.getInstance(conf, "line");
				
				//����map��reduce���Լ��ύ��jar
				job.setMapperClass(MapTask.class);
				job.setReducerClass(ReduceTask.class);
				job.setJarByClass(LineMr.class);
				
				//���������������
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				
				//��������Ŀ¼
				FileInputFormat.addInputPath(job, new Path("e:\\data\\line.txt"));
				FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\line"));
				
				//�ж��ļ��Ƿ����
				File file = new File("d:\\data\\out\\line");
				if(file.exists()){
					FileUtils.deleteDirectory(file);
				}
				
				//�ύ����
				boolean completion = job.waitForCompletion(true);
				System.out.println(completion?"������㣡����":"��ȥ��bug����");
		
		
		
	}
}
