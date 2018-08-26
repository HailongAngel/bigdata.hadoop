package day08.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JoinBean implements Writable{
	private String uid;
	private String age;
	private String gender;
	private String movieId;
	private String rating;
	
	private String table;
	
	
	
	
	public void set(String uid, String age, String gender, String movieId, String rating, String table) {
		this.uid = uid;
		this.age = age;
		this.gender = gender;
		this.movieId = movieId;
		this.rating = rating;
		this.table = table;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(uid);
		out.writeUTF(age);
		out.writeUTF(gender);
		out.writeUTF(movieId);
		out.writeUTF(rating);
		out.writeUTF(table);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		uid = in.readUTF();
		age = in.readUTF();
		gender = in.readUTF();
		movieId = in.readUTF();
		rating = in.readUTF();
		table = in.readUTF();
	}
	
	
	@Override
	public String toString() {
		return "JoinBean [uid=" + uid + ", age=" + age + ", gender=" + gender + ", movieId=" + movieId + ", rating="
				+ rating + ", table=" + table + "]";
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getMovieId() {
		return movieId;
	}

	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}

	public String getRating() {
		return rating;
	}

	public void setRating(String rating) {
		this.rating = rating;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	
	
}
