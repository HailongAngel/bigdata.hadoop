package log;

import java.util.Timer;

public class StartUp {
public static void main(String[] args) {
	Timer time = new Timer();
	//�ϴ�
	time.schedule(new ClollactionTask(), 0, 2*60*1000);
	//����
    time.schedule(new CleanTask(), 0, 2*60*60*1000);
}

}
