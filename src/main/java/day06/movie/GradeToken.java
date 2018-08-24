package day06.movie;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;

public class GradeToken {
  public  static class MapTask extends Mapper<LongWritable, Text, Text, Text>{
  }
  
  public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
	
	}
		
	
  }

