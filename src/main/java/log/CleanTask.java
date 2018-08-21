package log;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;

public class CleanTask extends TimerTask {
	SimpleDateFormat now = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
	Date date = new Date();

	@Override
	public void run() {
		try {
		File file = new File("D:/backDir");
		File[] listFiles = file.listFiles();
		for (File filedir : listFiles) {
			String name = filedir.getName();
            Date parse = now.parse(name);
            if(date.getTime() - parse.getTime()>= 60*1000) {
            	FileUtils.deleteDirectory(filedir);
            }
		}
	}
		catch (Exception e) {
			// TODO: handle exception
		}
	}
}
