package day07.top3;

import java.io.File;


import java.io.IOException;


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
public class TopN3 {
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
	protected void reduce(MovieBean key, Iterable<NullWritable> values,
			Reducer<MovieBean, NullWritable, MovieBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		int num=0;
		//虽然是一个空的，但是key能够根据迭代进行相应的得到对应空值的结果
		for (NullWritable nullWritable : values) {
			if(num>=20) {
				break;
			}
			num++;
			context.write(key, NullWritable.get());
		}
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
			job.setJarByClass(TopN3.class);
			job.setNumReduceTasks(2);  //ReduceTask的分区数
			job.setPartitionerClass(MyPartition.class);//加载设置分区的文件
			job.setGroupingComparatorClass(MyGroup.class);//设置分组
			

			// 设置输入输出类型
			job.setMapOutputKeyClass(MovieBean.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setOutputKeyClass(MovieBean.class);
			job.setOutputValueClass(NullWritable.class);
			// 判断文件是否存在
			File file = new File("d:\\data\\out\\movieTopn3");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			// 输入和输出目录
			FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\movieTopn3"));

			// 提交任务
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion ? "你很优秀！！！" : "滚去调bug！！");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}
}

