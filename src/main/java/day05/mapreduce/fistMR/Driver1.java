package day05.mapreduce.fistMR;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import day06.flow.FlowBean;
import day06.flow.FlowMR;
import day06.flow.FlowMR.MapTask;
import day06.flow.FlowMR.ReduceTask;

public class Driver1 {
	public static void main(String[] args) {
		try {
			//System.setProperty("HADOOP_USER_NAME", "SIMPLE");
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf);

			// ����map��reduce���Լ��ύ��jar
			job.setMapperClass(WordcountMapper.class);
			job.setReducerClass(WordcountReducer.class);
			job.setJarByClass(Driver1.class);
			// ���������������
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setCombinerClass(WordcountReducer.class);
			FileInputFormat.addInputPath(job, new Path("E:\\data\\word.txt"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\data1"));
			
			//�鿴�ύ֮ǰ�Ƿ����
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path("d:\\data\\out\\data1"))){
				fs.delete(new Path("d:\\data\\out\\data1"),true);
			}
			
			// �ύ֮���������״̬
			boolean completion = job.waitForCompletion(true);
			System.out.println(completion?"����ִ����ϣ�ûë��������":"���������⣬�����bug�ˣ��Ͻ��Ӱ���ԣ�����");
			

		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

	}

}
