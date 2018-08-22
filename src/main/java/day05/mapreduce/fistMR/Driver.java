package day05.mapreduce.fistMR;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); //���ؼ�Ⱥ�ϵ������ļ�	
		//conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		Job job = Job.getInstance(conf);
		
		//����job��map��reduce����һ����������������һ�������ύ
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		job.setJarByClass(Driver.class);
		
		//�����������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//������������Ŀ¼
		FileInputFormat.addInputPath(job, new Path("/wc.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/wc-output"));
		
		//�鿴�ύ֮ǰ�Ƿ����
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("/wordcount/wc-output"))){
			fs.delete(new Path("/wordcount/wc-output"),true);
		}
		
		// �ύ֮���������״̬
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"����ִ����ϣ�ûë��������":"���������⣬�����bug�ˣ��Ͻ��Ӱ���ԣ�����");
		
	}
}
