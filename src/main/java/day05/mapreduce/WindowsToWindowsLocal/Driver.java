package day05.mapreduce.WindowsToWindowsLocal;



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

public class Driver {
	public static void main(String[] args) throws Exception {
		//����ʹ���ĸ��û��ύ��
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		
		/**
		 * fs.defaultFS   Ĭ��ֵ file:///    �����ļ�ϵͳ
		 * mapreduce.framework.name   Ĭ��ֵ  local
		 */
		Job job = Job.getInstance(conf, "WindowsToWindowsLocal");
		
		//����map��reduce���Լ��ύ��jar
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		job.setJarByClass(Driver.class);
		
		//���������������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//��������Ŀ¼
		FileInputFormat.addInputPath(job, new Path("E:\\data\\word.txt"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\data\\out\\wc"));
		
		//�ж��ļ��Ƿ����
		File file = new File("d:\\data\\out\\wc");
		if(file.exists()){
			FileUtils.deleteDirectory(file);
		}
		
		//�ύ����
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?0:1);
		
	}
}
