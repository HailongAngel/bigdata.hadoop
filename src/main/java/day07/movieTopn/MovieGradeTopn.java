package day07.movieTopn;

import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.	hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

public class MovieGradeTopn {
  public static class MapTask extends Mapper<LongWritable, Text, Text, MovieBean>{
	  //ÿ�ζ����ݶ�Ҫ�½������ر��˷ѿռ䣬����������½�
	  ObjectMapper objectMapper = new ObjectMapper();
	  MovieBean movieBean = new MovieBean();
	  Text text = new Text();
	  @Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieBean>.Context context)
			throws IOException, InterruptedException {
	    MovieBean readValue = objectMapper.readValue(value.toString(), MovieBean.class);
		text.set(readValue.getMovie());
		// �����ｻ��maptask��kv���󣬻ᱻmaptask���л���洢�����Բ��õ��ĸ��ǵ�����
		context.write(text, readValue);
	}
  }
  
  public static class ReduceTask extends Reducer<Text, MovieBean, MovieBean, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<MovieBean> values,
			Reducer<Text, MovieBean, MovieBean, NullWritable>.Context context) throws IOException, InterruptedException {
		//��ȡtopn�Ĳ���
		  int topn = context.getConfiguration().getInt("movie.token.n", 3);
		  
		  ArrayList<MovieBean> beanList = new ArrayList<>();
		// reduce task�ṩ��values��������ÿ�ε������ظ����ǵĶ���ͬһ������ֻ��set�˲�ͬ��ֵ
		  for (MovieBean movieBean : values) {
		     
			// ����һ���µĶ������洢���ε���������ֵ
			  MovieBean newBean = new MovieBean();
			  newBean.set(movieBean.getMovie(),movieBean.getRate(),movieBean.getTimeStamp(),movieBean.getUid());
			  beanList.add(newBean);
		}
		// ��beanList�е�orderBean�������򣨰��ܽ���С��������,����ܽ����ͬ�������Ʒ���ƣ�
					Collections.sort(beanList);
	         		for (int i=0;i<Math.min(topn, beanList.size());i++) {
						context.write(beanList.get(i), NullWritable.get());
					}
					
	}
}
  public static void main(String[] args) {
		try {
			//System.setProperty("HADOOP_USER_NAME", "SIMPLE");
			Configuration conf = new Configuration();
			//��̬����top.n����
			conf.setInt("movie.top.n", 20);
			Job job = Job.getInstance(conf);

			// ����map��reduce���Լ��ύ��jar
			job.setMapperClass(MapTask.class);
			job.setReducerClass(ReduceTask.class);
			job.setJarByClass(MovieGradeTopn.class);

			// ���������������
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(MovieBean.class);

			job.setOutputKeyClass(MovieBean.class);
			job.setOutputValueClass(NullWritable.class);
			// �ж��ļ��Ƿ����
			File file = new File("d:\\data\\out\\movieTopn");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			// ��������Ŀ¼
			FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\movieTopn"));

			// �ύ����
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion ? "������㣡����" : "��ȥ��bug����");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}
}
