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
	  //每次读数据都要新建对象，特别浪费空间，所以在外边新建
	  ObjectMapper objectMapper = new ObjectMapper();
	  MovieBean movieBean = new MovieBean();
	  Text text = new Text();
	  @Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieBean>.Context context)
			throws IOException, InterruptedException {
	    MovieBean readValue = objectMapper.readValue(value.toString(), MovieBean.class);
		text.set(readValue.getMovie());
		// 从这里交给maptask的kv对象，会被maptask序列化后存储，所以不用担心覆盖的问题
		context.write(text, readValue);
	}
  }
  
  public static class ReduceTask extends Reducer<Text, MovieBean, MovieBean, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<MovieBean> values,
			Reducer<Text, MovieBean, MovieBean, NullWritable>.Context context) throws IOException, InterruptedException {
		//获取topn的参数
		  int topn = context.getConfiguration().getInt("movie.token.n", 3);
		  
		  ArrayList<MovieBean> beanList = new ArrayList<>();
		// reduce task提供的values迭代器，每次迭代返回给我们的都是同一个对象，只是set了不同的值
		  for (MovieBean movieBean : values) {
		     
			// 构造一个新的对象，来存储本次迭代出来的值
			  MovieBean newBean = new MovieBean();
			  newBean.set(movieBean.getMovie(),movieBean.getRate(),movieBean.getTimeStamp(),movieBean.getUid());
			  beanList.add(newBean);
		}
		// 对beanList中的orderBean对象排序（按总金额大小倒序排序,如果总金额相同，则比商品名称）
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
			//动态设置top.n多少
			conf.setInt("movie.top.n", 20);
			Job job = Job.getInstance(conf);

			// 设置map和reduce，以及提交的jar
			job.setMapperClass(MapTask.class);
			job.setReducerClass(ReduceTask.class);
			job.setJarByClass(MovieGradeTopn.class);

			// 设置输入输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(MovieBean.class);

			job.setOutputKeyClass(MovieBean.class);
			job.setOutputValueClass(NullWritable.class);
			// 判断文件是否存在
			File file = new File("d:\\data\\out\\movieTopn");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			// 输入和输出目录
			FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\movieTopn"));

			// 提交任务
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion ? "你很优秀！！！" : "滚去调bug！！");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}
}
