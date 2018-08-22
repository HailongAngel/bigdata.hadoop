package day05.mapreduce.line;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *  KEYIN ：是map task读取到的数据的key的类型，是一行的起始偏移量Long
 *  VALUEIN:是map task读取到的数据的value的类型，是一行的内容String
 * KEYOUT：是用户的自定义map方法要返回的结果kv数据的key的类型，在wordcount逻辑中，我们需要返回的是单词String
 * VALUEOUT:是用户的自定义map方法要返回的结果kv数据的value的类型，在wordcount逻辑中，我们需要返回的是整数Integer
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
