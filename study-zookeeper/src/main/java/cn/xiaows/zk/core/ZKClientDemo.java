package cn.xiaows.zk.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class ZKClientDemo {
	ZooKeeper zk = null;

	@Before
	public void init() throws Exception {
		// 构造一个连接zookeeper的客户端对象
		// zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, null);
		zk = new ZooKeeper("zk-1:2181,zk-2:2181,zk-3:2181", 2000, null);
	}

	// @Test
	// public void test() throws Exception {
	// 	int i = new Random().nextInt(10) + 1;
	// 	Thread.sleep(i);
	// 	// // 1.向zk注册
	// 	// String s = zk.create("/task", "1", Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
	// 	Stat stat = zk.exists("/task", false);
	// 	if (stat == null) {// 节点不存在 则创建
	// 		// task 0 表示没有任务在执行, 1 表示任务正在执行中
	// 		zk.create("/task", "1".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	// 		// do you jobs
	// 		// ...
	// 		for (int j = 0; j < 10; j++) {
	// 			Thread.sleep(2000);
	// 			System.out.println(j);
	//
	// 		}
	//
	// 		// 最后更新节点数据 0表示当前task执行完毕
	// 		zk.setData("/task", "0".getBytes("UTF-8"), -1);
	// 	} else {// 节点存在
	// 		byte[] data = zk.getData("/task", false, null);
	// 		if (Integer.parseInt(String.valueOf(data)) == 0) {
	// 			//此时 表示当前没有任何机器在执行task,自己可以执行task
	// 			zk.setData("/task", "1".getBytes("UTF-8"), -1);
	// 			// do you jobs
	// 			// ...
	// 			for (int j = 0; j < 10000; j++) {
	// 				System.out.println(j);
	// 			}
	// 			// 最后更新节点数据 0表示当前task执行完毕
	// 			zk.setData("/task", "0".getBytes("UTF-8"), -1);
	// 		} else {
	// 			//1 表示 另一台在执行task中,我直接退出
	// 		}
	// 	}
	//
	// 	zk.close();
	// }

	public static void main(String[] args) throws Exception {
		ZooKeeper zk = new ZooKeeper("zk-1:2181,zk-2:2181,zk-3:2181", 10000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath() + "\t" + event.getType() + "\t" + event.getState() + "\t" + event.getWrapper());
				// log.info(event.getPath() + "\t" + event.getType() + "\t" + event.getState() + "\t" + event.getWrapper());
			}
		});
		int i = new Random().nextInt(10000);
		System.out.println("sleeping " + i);
		Thread.sleep(i);
		System.out.println("sleep done");
		// // 1.向zk注册
		// String s = zk.create("/task", "1", Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
		Stat stat = zk.exists("/task", false);
		if (stat == null) {// 节点不存在 则创建
			// task 0 表示没有任务在执行, 1 表示任务正在执行中
			String s = zk.create("/task", "1".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			// do you jobs
			// ...
			for (int j = 0; j < 10; j++) {
				Thread.sleep(2000);
				System.out.println(j);
			}
			// 最后更新节点数据 0表示当前task执行完毕
			zk.setData("/task", "0".getBytes("UTF-8"), -1);
		} else {// 节点存在
			byte[] data = zk.getData("/task", false, null);
			if ("0".equals(new String(data, "UTF-8"))) {
				//此时 表示当前没有任何机器在执行task,自己可以执行task
				System.out.println("此时 表示当前没有任何机器在执行task,自己可以执行task");
				zk.setData("/task", "1".getBytes("UTF-8"), -1);
				// do you jobs
				// ...
				for (int j = 0; j < 10; j++) {
					Thread.sleep(2000);
					System.out.println(j);
				}
				// 最后更新节点数据 0表示当前task执行完毕
				zk.setData("/task", "0".getBytes("UTF-8"), -1);
			} else {
				//1 表示 另一台在执行task中,我直接退出
				System.out.println("1 表示 另一台在执行task中,我直接退出");
			}
		}
		System.out.println("============================");
		zk.close();
	}
}
