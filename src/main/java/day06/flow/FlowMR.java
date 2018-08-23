package day06.flow;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowMR {
	public static class MapTask extends Mapper<LongWritable, Text, Text, FlowBean> {
		public String reg(String url) {
			Pattern pattern = Pattern.compile("(\\w+\\.)?(\\w+\\.){1}\\w+");//����������ʽ
			Matcher matcher = pattern.matcher(url);
			while(matcher.find()){
				String newUrl = matcher.group();
				return newUrl;
			}
			return null;
					
					
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			try {

				String[] split = value.toString().split("\t")[1].split(" ");
				long up = Long.parseLong(split[1]);
				long down = Long.parseLong(split[2]);
				String url = reg(split[0]);
				FlowBean fb = new FlowBean();
				fb.set(up, down);
				context.write(new Text(url), fb);

			} catch (Exception e) {

			}
			// TODO: handle exception
		}
	}

	public static class ReduceTask extends Reducer<Text, FlowBean, Text, FlowBean> {

		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,
				Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
			long up = 0;
			long down = 0;
			FlowBean fb = new FlowBean();
			for (FlowBean flowBean : values) {
				up += flowBean.getUp();
				down += flowBean.getDown();
			}
			fb.set(up, down);
			context.write(key, fb);
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
			job.setJarByClass(FlowMR.class);

			// ���������������
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlowBean.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FlowBean.class);
			// �ж��ļ��Ƿ����
			File file = new File("d:\\data\\out\\http");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			// ��������Ŀ¼
			FileInputFormat.addInputPath(job, new Path("E:/data/DATA.txt"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\http"));

			// �ύ����
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion ? "������㣡����" : "��ȥ��bug����");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}
}
