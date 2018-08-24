package day07.topnhomework;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitition extends Partitioner<MovieBean, NullWritable>{

	@Override
	public int getPartition(MovieBean key, NullWritable value, int numpartitions) {
		return (key.getUid().hashCode() & Integer.MAX_VALUE)%numpartitions;
	}

}
