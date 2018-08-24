package day07.topnhomework;


import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
//��Map��һֱʹ�ö��󲻻Ḳ�ǣ������л���洢�����Բ��õ��ĸ��ǵ�����
public class MovieBean implements WritableComparable<MovieBean>{
	private String movie;
	private int rate;
	private String timeStamp;
	private String uid;
	
	public void  set(String movie, int rate, String timeStamp, String uid) {
		this.movie = movie;
		this.rate = rate;
		this.timeStamp = timeStamp;
		this.uid = uid;
	}
	
	public String getMovie() {
		return movie;
	}
	public void setMovie(String movie) {
		this.movie = movie;
	}
	public int getRate() {
		return rate;
	}
	public void setRate(int rate) {
		this.rate = rate;
	}
	public String getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	@Override
	public String toString() {
		return "MovieBean [movie=" + movie + ", rate=" + rate + ", timeStamp=" + timeStamp + ", uid=" + uid + "]";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.movie = in.readUTF();
		this.rate = in.readInt();
		this.timeStamp = in.readUTF();
		this.uid = in.readUTF();
				
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.movie);
		out.writeInt(this.rate);
		out.writeUTF(this.timeStamp);
		out.writeUTF(this.uid);
		
	}
	// �ȽϹ����ȱ����֣������ͬ���ٱȵ�Ӱ����
	@Override
	public int compareTo(MovieBean o) {
		// TODO Auto-generated method stub
		//return Integer.compare(o.getRate(), this.getRate()==0?this.getMovie().compareTo(o.getMovie()):Integer.compare(o.getRate(), this.getRate()));
		if(o.getRate() - this.getRate() == 0) {
			return o.getUid().compareTo(this.getUid());
		}
		else {
			return o.getRate() - this.getRate();
		}
	
	}
	
}
