package cn.xiaows.zk.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

@Slf4j
public class ZKDemo {

	public void test() throws Exception {
		// 创建一个连接
		String connectStr = "172.18.1.5:2181,172.18.1.6:2181,172.18.1.7:2181";
		ZooKeeper zk = new ZooKeeper(connectStr, 60000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath() + "\t" + event.getType() + "\t" + event.getState() + "\t" + event.getWrapper());
				// log.info(event.getPath() + "\t" + event.getType() + "\t" + event.getState() + "\t" + event.getWrapper());
			}
		});
		// 查看根节点
		log.info("ls / => " + zk.getChildren("/", true));

		// 创建一个目录节点
		if (zk.exists("/node", true) == null) {
			log.info("create /node conan");
			zk.create("/node", "conan".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			log.info("get /node => " + new String(zk.getData("/node", false, null)));
			log.info("ls / => " + zk.getChildren("/", true));
		}

		// 创建一个子目录节点
		if (zk.exists("/node/sub1", true) == null) {
			log.info("create /node/sub1 sub1");
			zk.create("/node/sub1", "sub1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			log.info("ls /node => " + zk.getChildren("/node", true));
		}

		// 修改节点数据
		if (zk.exists("/node", true) != null) {
			zk.setData("/node", "changed".getBytes(), -1);
			log.info("get /node => " + new String(zk.getData("/node", false, null)));
		}

		// 删除节点
		if (zk.exists("/node/sub1", true) != null) {
			zk.delete("/node/sub1", -1);
			zk.delete("/node", -1);
			log.info("ls / => " + zk.getChildren("/", true));
		}
		// 关闭连接
		zk.close();
	}

	public static void main(String[] args) throws Exception {
		new ZKDemo().test();
	}

}
