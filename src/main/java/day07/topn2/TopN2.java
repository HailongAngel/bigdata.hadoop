package day07.topn2;

import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;

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
/**
 * 电影评分的优化
 * @author Hailong
 *
 */
public class TopN2 {
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
		//利用堆排序去节省空间，把他限制在20个对象
		TreeSet<MovieBean> tree = new TreeSet<>(new Comparator<MovieBean>() {
			@Override
			//
			public int compare(MovieBean o1, MovieBean o2) {//堆排序的原则，先按评分排，后按Uid排
				// TODO Auto-generated method stub
				if(o1.getRate()-o2.getRate() == 0) {
					return o1.getUid().compareTo(o2.getUid());
				}
				else {
					return o1.getRate()-o2.getRate();
				}
			}
		});
		for (MovieBean movieBean : values) {
			MovieBean mBean = new MovieBean();
			mBean.set(movieBean.getMovie(), movieBean.getRate(), movieBean.getTimeStamp(), movieBean.getUid());
			if(tree.size()<=20) {  //先把堆的前20填满
				tree.add(mBean);
			}
			else {//将新添加的元素进行更新，如果小于堆顶直接丢弃，大于堆顶交换
				MovieBean first = tree.first();
				if(first.getRate()<mBean.getRate()) {
					tree.remove(first);
					tree.add(mBean);
				}
			}
		}
		for (MovieBean movieBean : tree) {
			context.write(movieBean, NullWritable.get());
			
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
			job.setJarByClass(TopN2.class);

			// 设置输入输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(MovieBean.class);

			job.setOutputKeyClass(MovieBean.class);
			job.setOutputValueClass(NullWritable.class);
			// 判断文件是否存在
			File file = new File("d:\\data\\out\\movieTopn2");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			// 输入和输出目录
			FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\movieTopn2"));

			// 提交任务
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion ? "你很优秀！！！" : "滚去调bug！！");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}
}

