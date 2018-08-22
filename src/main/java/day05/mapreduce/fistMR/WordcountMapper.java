package day05.mapreduce.fistMR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.MAP;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String string = value.toString();
		String[] words = string.split(" ");
		for (String word : words) {
			
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
