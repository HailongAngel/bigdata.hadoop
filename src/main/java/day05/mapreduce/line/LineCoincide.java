package day05.mapreduce.line;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *  KEYIN ����map task��ȡ�������ݵ�key�����ͣ���һ�е���ʼƫ����Long
 *  VALUEIN:��map task��ȡ�������ݵ�value�����ͣ���һ�е�����String
 * KEYOUT�����û����Զ���map����Ҫ���صĽ��kv���ݵ�key�����ͣ���wordcount�߼��У�������Ҫ���ص��ǵ���String
 * VALUEOUT:���û����Զ���map����Ҫ���صĽ��kv���ݵ�value�����ͣ���wordcount�߼��У�������Ҫ���ص�������Integer
 * @author Hailong
 *
 */
public class LineCoincide extends Mapper<LongWritable,Text , IntWritable, IntWritable>{
	@Override
	protected void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		try {
			
			String[] line = values.toString().split(",");
			for(int i =Integer.parseInt(line[0]);i<=Integer.parseInt(line[1]);i++) {
				context.write(new IntWritable(i) ,new IntWritable(1));
			}
		}
		catch (Exception e) {
		}
		
		
	}
      
}
