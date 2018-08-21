package day04;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapTask {
	public static void main(String[] args) throws Exception {
		/**
		 * taskId 标示哪台机器运行的
		 * file  统计哪个文件的
		 * startOffSet 从哪个位置开始的
		 * length   读多长
		 */
		int taskId =Integer.parseInt(args[0]);
		String file = args[1];
		long startOffSet = Long.parseLong(args[2]);
		long lenth = Long.parseLong(args[3]);
		
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration(), "root");
		FSDataInputStream inputStream = fs.open(new Path(file));
		
		//输出文件
		FSDataOutputStream out_tmp_1 = fs.create(new Path("/wordcount/tmp/part-m"+taskId+"-1"));
		FSDataOutputStream out_tmp_2 = fs.create(new Path("/wordcount/tmp/part-m"+taskId+"-2"));
		
		//定位到从哪里读
		inputStream.seek(startOffSet);
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		
		// 除了taskid为1的能读第一行，后面的task都需要 跳过一行
		if(taskId!=1){
			br.readLine();
		}
		
		long count = 0;
		String line = null;
		while((line = br.readLine())!=null){
			String[] split = line.split(" ");
			for (String word : split) {
				//相同的字符串相同的hashcode   hello 奇数   hello 偶数  
				if(word.hashCode()%2 == 0){
					out_tmp_1.write((word+"\t"+1+"\n").getBytes());
				}else{
					out_tmp_2.write((word+"\t"+1+"\n").getBytes());
				}
			}
			//累加每行的数据长度   多读一行
			count += line.length()+1;
			if(count>lenth){
				break;
			}
		}
		
	    inputStream.close();
		out_tmp_1.close();
		out_tmp_2.close();
		br.close();
		fs.close();
		
	}
}
