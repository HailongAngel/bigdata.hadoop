package day07.top3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{
	public MyGroup() {
		super(MovieBean.class,true); //使用自己的类，之前系统默认为false
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {//专门用于指定使用什么进行分组
		MovieBean bean1 = (MovieBean)a;
		MovieBean bean2 = (MovieBean)b;
		return bean1.getMovie().compareTo(bean2.getMovie());
	}
	
	

}
