package day09;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * С���״����Խ�ʡʱ��
 * ��map�׶ξͿ���ʵ��
 * ˼·��
 * ��setup�׶ν�С��ָ�����key�ҳ�������map�׶θ���key�Ӷ��ҵ����ļ���
 * �Լ�����Ӧ�����ݣ������ݷ�����󣬴Ӷ�ʵ��ƴ��
 * ������key
 */

public class JoinMR {
	public static class MapTask extends Mapper<LongWritable, Text, JoinBean, NullWritable>{
		Map<String,String> map = new HashMap<>();
		@Override
		protected void setup(Context context)  
				throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String smallTableName = conf.get("smallTableName");
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream open = fs.open(new Path(smallTableName));
		BufferedReader br = new BufferedReader(new InputStreamReader(open));
		String line = null;
		while((line = br.readLine())!=null) {
			String[] split = line.split("::");
			map.put(split[0], line);
		}
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("::");
			JoinBean joinBean = new JoinBean();
			String[] line = map.get(split[0]).split("::");
			joinBean.set(split[0], line[2], line[1], split[1], split[2], "null");
			context.write(joinBean,NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{

		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		// 1������job����ʱҪ���ʵ�Ĭ���ļ�ϵͳ
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		// 2������job�ύ����ȥ����
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "hadoop01");
		// 3�����Ҫ��windowsϵͳ���������job�ύ�ͻ��˳�������Ҫ�������ƽ̨�ύ�Ĳ���
		conf.set("mapreduce.app-submission.cross-platform","true");
	    conf.set("smallTableName", args[2]);
	   
	   Job job = Job.getInstance(conf);
	 // 1����װ������jar�����ڵ�λ��
	   job.setJar("C:\\Users\\Hailong\\Desktop\\w2.jar"); 
	   job.setMapperClass(MapTask.class);
	   job.setOutputKeyClass(JoinBean.class);
	   job.setOutputValueClass(NullWritable.class);
	   
	   FileSystem fs = FileSystem.get(conf);
	   if(!fs.exists(new Path("/data/out/join"))) {
		  fs.delete(new Path("/data/out/join"),true);
	   }
	   
	   FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	    // 5����װ��������Ҫ������reduce task������
		job.setNumReduceTasks(2);
	   
	   boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"����ִ����ϣ�ûë��������":"���������⣬�����bug�ˣ��Ͻ��Ӱ���ԣ�����");
   }
   

	
	
   
}
