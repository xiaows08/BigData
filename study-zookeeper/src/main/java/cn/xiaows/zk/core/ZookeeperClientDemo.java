package cn.xiaows.zk.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 *
 * @author: xiaows
 * @create: 2019-03-02 11:35
 * @version: v1.0
 */
public class ZookeeperClientDemo {

	private ZooKeeper zk = null;

	@Before
	public void init() throws IOException {
		zk = new ZooKeeper("localhost:2181", 5000, null);
	}

	@After
	public void close() throws InterruptedException {
		zk.close();
	}

	@Test
	public void create() throws KeeperException, InterruptedException, UnsupportedEncodingException {
		String s = zk.create("/idea", "我爱你".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
	}

	@Test
	public void get() throws KeeperException, InterruptedException, UnsupportedEncodingException {
		byte[] data = zk.getData("/idea", false, null);
		System.out.println(new String(data, "UTF-8"));
	}

	@Test
	public void update() throws UnsupportedEncodingException, KeeperException, InterruptedException {
		Stat stat = zk.setData("/idea", "我不爱你".getBytes("UTF-8"), -1);
	}

	@Test
	public void getChilden() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren("/idea", false);
		children.forEach(s -> System.out.println(s));
	}

	@Test
	public void rm() throws KeeperException, InterruptedException {
		zk.delete("/idea/cc", -1);
	}
}
