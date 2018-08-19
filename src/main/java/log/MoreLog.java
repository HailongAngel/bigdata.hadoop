package log;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MoreLog {
public static void main(String[] args) throws Exception {
	Logger logger = LogManager.getLogger(MoreLog.class);
	System.out.println("开始打印日志了：");
	while(true) {
		logger.info("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
		Thread.sleep(1);
	}
}
}
