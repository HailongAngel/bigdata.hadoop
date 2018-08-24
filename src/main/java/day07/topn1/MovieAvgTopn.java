package day07.topn1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;

public class MovieAvgTopn {
  public static class MapTask extends Mapper<LongWritable, Text, Text, MovieBean>{
	  Text text = new Text();
	  ObjectMapper objectMapper = new ObjectMapper();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieBean>.Context context)
			throws IOException, InterruptedException {
		MovieBean readValue = objectMapper.readValue(value.toString(), MovieBean.class);
		text.set(readValue.getMovie());
		context.write(text, readValue);
	}
  }
  
  public static class ReduceTask extends Reducer<Text, MovieBean, MovieBean, NullWritable>{
	  @Override
	protected void reduce(Text movieId, Iterable<MovieBean> movieBeans,
			Reducer<Text, MovieBean, MovieBean, NullWritable>.Context context) throws IOException, InterruptedException {
		  List<MovieBean> listBean = new ArrayList<>();
		  int sum = 0;
		  int count = 0;
		for (MovieBean movieBean : movieBeans) {
			sum += movieBean.getRate();
			count++;
			movieBean.set(movieBean.getMovie(), sum, movieBean.getTimeStamp(), movieBean.getUid());
			listBean.add(movieBean);
		}
		
		Collections.sort(listBean);
	}
  }
}
