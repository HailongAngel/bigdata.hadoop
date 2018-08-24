package day07.top3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{
	public MyGroup() {
		super(MovieBean.class,true); //ʹ���Լ����֮࣬ǰϵͳĬ��Ϊfalse
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {//ר������ָ��ʹ��ʲô���з���
		MovieBean bean1 = (MovieBean)a;
		MovieBean bean2 = (MovieBean)b;
		return bean1.getMovie().compareTo(bean2.getMovie());
	}
	
	

}
