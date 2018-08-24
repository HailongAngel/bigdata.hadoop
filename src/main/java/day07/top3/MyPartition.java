package day07.top3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区的：
 * 把想要的数据分到相同的reduce里面
 * @author Hailong
 *
 */
public class MyPartition extends Partitioner<MovieBean, NullWritable>{
	/**
	 * numPartition代表多少个reduceTask
	 * key map 端输出的key
	 * value map端输出的value
	 */
	@Override
	public int getPartition(MovieBean key, NullWritable value, int numPartitions) {
		//进行分块，& Integer.MAX_VALUE的代码是让前面的值为正
		//想要指定那块，在后边赋值就OK
		return (key.getMovie().hashCode() & Integer.MAX_VALUE)%numPartitions;
	}

	
}
