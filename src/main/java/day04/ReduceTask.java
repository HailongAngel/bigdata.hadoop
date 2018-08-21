package day04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ReduceTask {
public static void main(String[] args) throws Exception {
	
	int taskId = Integer.parseInt(args[0]); //Ҫ������̨�����ϵ�
	
	Map<String,Integer> map = new HashMap<>();
	
	FileSystem fs =FileSystem.get(new URI("hdfs://hadoop01:9000"),new Configuration(), "root");
	//ѭ���ݹ�ÿһ���ļ��������ҵ��Լ���Ҫ��
	RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/wordcount/tmp/"), true);
	while(listFiles.hasNext()) {
		LocatedFileStatus file = listFiles.next();
		int count = 0;
		//�ҵ�����ͬtaskId�ŵĽ��м���
		if(file.getPath().getName().endsWith("-"+taskId)) {
			//��ʼ��ȡ��ͬ�ļ�
			FSDataInputStream inputStream = fs.open(file.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			String str = null;
			if((str = br.readLine())!=null) {
				//���м�����ͬ��
				String[] split = str.split("\t");
				map.getOrDefault(split[0], 0);
				count += Integer.parseInt(split[1]);
				map.put(split[0], count);
			}
			br.close();
			inputStream.close();
		}
	}
	//�����д�뵽HDFS��
	FSDataOutputStream outputStream = fs.create(new Path("/wordcount/ret/part-r-"+taskId));
	Set<Entry<String, Integer>> entrySet = map.entrySet();
	for (Entry<String, Integer> entry : entrySet) {
		outputStream.write((entry.getKey()+"="+entry.getValue()+"\n").getBytes());
		
	}
	
	outputStream.close();
	fs.close();
	
}
}
