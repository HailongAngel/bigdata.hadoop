package day09.test1.movieAvg;

public class Bean {
 private String movie;
 private int rate;
 
public Bean(String movie, int rate) {
	super();
	this.movie = movie;
	this.rate = rate;
}
@Override
public String toString() {
	return "Bean [movie=" + movie + ", rate=" + rate + "]";
}
 
}
