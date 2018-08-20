package log;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ��־�ռ����裺 1������־Ŀ¼����鿴����Ҫ�ϴ��ģ�.1 .2 .....�� 2:����Ҫ�ϴ����ļ��ƶ������ϴ�Ŀ¼
 * 3���ϴ���hdfs��(/log/2018-8-20/xxx.log) 4:�ƶ�������Ŀ¼
 * @author root
 *
 */
public class ClollactionTask extends TimerTask {
	
	@Override
	public void run() {
		System.out.println("��ʼִ������");
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
			String datetime = format.format(new Date());
			
			// 1:�鿴��Щ�ļ�����Ҫ�ϴ���
			File logDir = new File("d:/testlog/");
			File[] listFiles = logDir.listFiles(new FilenameFilter() {
				// FileNameFilter �ǹ���Щ�ļ��ܹ���ȡ��
				@Override
				public boolean accept(File dir, String name) {
					return name.startsWith("test.log.");
				}
			});

			// 2:���ļ��ƶ������ϴ�Ŀ¼
			for (File file : listFiles) {
				// file.renameTo(new File(""));
				FileUtils.moveFileToDirectory(file, new File("d:/waitUpLoad"), true);
			}
			//3:�����ϴ����ļ�����ϴ���hdfs�ϣ����ƶ�������Ŀ¼
			FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration(), "root");
			Path detPath = new Path("/log/"+datetime.substring(0, 10));
			//�жϴ��ϴ���Ŀ¼�Ƿ���ڣ��������򴴽�һ����hdfs�ϵ��ļ��У�
			boolean exists = fs.exists(detPath);
			if(!exists){
				fs.mkdirs(detPath);
			}
			
			//�жϱ���Ŀ¼�Ƿ����
			File backDir = new File("d:/backDir/"+datetime);
			boolean exists2 = backDir.exists();
			if(!exists2){
				backDir.mkdirs();
			}
			
			//�õ��ϴ�������һ�������ϵ���־�ļ�
			String hostName = InetAddress.getLocalHost().getHostName();
			
			//4:�������ϴ���Ŀ¼
			File file = new File("d:/waitUpLoad");
			File[] listFile = file.listFiles();
			for (File f : listFile) {
				//�ϴ���hdfs��
				fs.copyFromLocalFile(new Path(f.getPath()), new Path(detPath,hostName+"_"+f.getName()+"_"+System.currentTimeMillis()));
				
				//cp������Ŀ¼
				FileUtils.moveFileToDirectory(f, backDir, true);
			}
			fs.close();
			
		} catch (Exception e) {
			e.getStackTrace();
		}

	}
}
