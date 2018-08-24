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
 * ��Ӱ���ֵ��Ż�
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
		//��Ȼ��һ���յģ�����key�ܹ����ݵ���������Ӧ�ĵõ���Ӧ��ֵ�Ľ��
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

			// ����map��reduce���Լ��ύ��jar
			job.setMapperClass(MapTask.class);
			job.setReducerClass(ReduceTask.class);
			job.setJarByClass(TopN3.class);
			job.setNumReduceTasks(2);  //ReduceTask�ķ�����
			job.setPartitionerClass(MyPartition.class);//�������÷������ļ�
			job.setGroupingComparatorClass(MyGroup.class);//���÷���
			

			// ���������������
			job.setMapOutputKeyClass(MovieBean.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setOutputKeyClass(MovieBean.class);
			job.setOutputValueClass(NullWritable.class);
			// �ж��ļ��Ƿ����
			File file = new File("d:\\data\\out\\movieTopn3");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
			// ��������Ŀ¼
			FileInputFormat.addInputPath(job, new Path("D:\\data\\in\\movie\\rating.json"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\movieTopn3"));

			// �ύ����
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion ? "������㣡����" : "��ȥ��bug����");

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}
}

