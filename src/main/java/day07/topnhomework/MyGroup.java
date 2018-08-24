package day07.topnhomework;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{
        public MyGroup() {
        	super(MovieBean.class,true);
        }

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			MovieBean bean1 = (MovieBean)a;
			MovieBean bean2 = (MovieBean)b;
			return bean1.getUid().compareTo(bean2.getUid());
			
		}
        
        
}
