package log;

import java.util.Timer;

public class StartUp {
public static void main(String[] args) {
	Timer time = new Timer();
	//上传
	time.schedule(new ClollactionTask(), 0, 2*60*1000);
	//清理
    time.schedule(new ClollactionTask1(), 0, 60*1000);
}

}
