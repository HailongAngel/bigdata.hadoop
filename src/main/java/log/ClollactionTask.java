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
 * 日志收集步骤： 1：从日志目录里面查看哪需要上传的（.1 .2 .....） 2:把需要上传的文件移动到待上传目录
 * 3：上传到hdfs上(/log/2018-8-20/xxx.log) 4:移动到备份目录
 * @author root
 *
 */
public class ClollactionTask extends TimerTask {
	
	@Override
	public void run() {
		System.out.println("开始执行任务");
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
			String datetime = format.format(new Date());
			
			// 1:查看哪些文件是需要上传的
			File logDir = new File("d:/testlog/");
			File[] listFiles = logDir.listFiles(new FilenameFilter() {
				// FileNameFilter 是过哪些文件能够获取的
				@Override
				public boolean accept(File dir, String name) {
					return name.startsWith("test.log.");
				}
			});

			// 2:将文件移动到待上传目录
			for (File file : listFiles) {
				// file.renameTo(new File(""));
				FileUtils.moveFileToDirectory(file, new File("d:/waitUpLoad"), true);
			}
			//3:将待上传的文件逐个上传到hdfs上，并移动到备份目录
			FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration(), "root");
			Path detPath = new Path("/log/"+datetime.substring(0, 10));
			//判断待上传的目录是否存在，不存在则创建一个（hdfs上的文件夹）
			boolean exists = fs.exists(detPath);
			if(!exists){
				fs.mkdirs(detPath);
			}
			
			//判断备份目录是否存在
			File backDir = new File("d:/backDir/"+datetime);
			boolean exists2 = backDir.exists();
			if(!exists2){
				backDir.mkdirs();
			}
			
			//得到上传的是哪一个服务上的日志文件
			String hostName = InetAddress.getLocalHost().getHostName();
			
			//4:遍历待上传的目录
			File file = new File("d:/waitUpLoad");
			File[] listFile = file.listFiles();
			for (File f : listFile) {
				//上传到hdfs上
				fs.copyFromLocalFile(new Path(f.getPath()), new Path(detPath,hostName+"_"+f.getName()+"_"+System.currentTimeMillis()));
				
				//cp到备份目录
				FileUtils.moveFileToDirectory(f, backDir, true);
			}
			fs.close();
			
		} catch (Exception e) {
			e.getStackTrace();
		}

	}
}
