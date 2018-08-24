package day07.top3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * �����ģ�
 * ����Ҫ�����ݷֵ���ͬ��reduce����
 * @author Hailong
 *
 */
public class MyPartition extends Partitioner<MovieBean, NullWritable>{
	/**
	 * numPartition������ٸ�reduceTask
	 * key map �������key
	 * value map�������value
	 */
	@Override
	public int getPartition(MovieBean key, NullWritable value, int numPartitions) {
		//���зֿ飬& Integer.MAX_VALUE�Ĵ�������ǰ���ֵΪ��
		//��Ҫָ���ǿ飬�ں�߸�ֵ��OK
		return (key.getMovie().hashCode() & Integer.MAX_VALUE)%numPartitions;
	}

	
}
