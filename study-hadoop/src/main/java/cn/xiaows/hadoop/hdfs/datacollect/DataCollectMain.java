package cn.xiaows.hadoop.hdfs.datacollect;

import java.util.Timer;

public class DataCollectMain {

	public static void main(String[] args) {
		Timer timer = new Timer();
		timer.schedule(new CollectTask(), 0, 5*60*1000L);
		// timer.scheduleAtFixedRate(new CollectTask(), 0, 5*60*1000L);
	}
}
