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
		 * taskId ��ʾ��̨�������е�
		 * file  ͳ���ĸ��ļ���
		 * startOffSet ���ĸ�λ�ÿ�ʼ��
		 * length   ���೤
		 */
		int taskId =Integer.parseInt(args[0]);
		String file = args[1];
		long startOffSet = Long.parseLong(args[2]);
		long lenth = Long.parseLong(args[3]);
		
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration(), "root");
		FSDataInputStream inputStream = fs.open(new Path(file));
		
		//����ļ�
		FSDataOutputStream out_tmp_1 = fs.create(new Path("/wordcount/tmp/part-m"+taskId+"-1"));
		FSDataOutputStream out_tmp_2 = fs.create(new Path("/wordcount/tmp/part-m"+taskId+"-2"));
		
		//��λ���������
		inputStream.seek(startOffSet);
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		
		// ����taskidΪ1���ܶ���һ�У������task����Ҫ ����һ��
		if(taskId!=1){
			br.readLine();
		}
		
		long count = 0;
		String line = null;
		while((line = br.readLine())!=null){
			String[] split = line.split(" ");
			for (String word : split) {
				//��ͬ���ַ�����ͬ��hashcode   hello ����   hello ż��  
				if(word.hashCode()%2 == 0){
					out_tmp_1.write((word+"\t"+1+"\n").getBytes());
				}else{
					out_tmp_2.write((word+"\t"+1+"\n").getBytes());
				}
			}
			//�ۼ�ÿ�е����ݳ���   ���һ��
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
