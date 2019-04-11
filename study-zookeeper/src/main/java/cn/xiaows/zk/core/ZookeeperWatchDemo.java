package cn.xiaows.zk.core;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * desc:
 *
 * @author: xiaows
 * @email: xiaows08@163.com
 * @create: 2019-03-02 11:55
 * @version: v1.0
 */
public class ZookeeperWatchDemo {
	private ZooKeeper zk = null;

	@Before
	public void init() throws IOException {
		zk = new ZooKeeper("localhost:2181", 5000, event -> {
			// default watcher
			if (event.getState() == Watcher.Event.KeeperState.SyncConnected && event.getType() == Watcher.Event.EventType.NodeDataChanged) {
				System.out.println(event.getPath() + " the data has changed!");
				try {
					byte[] data = zk.getData("/idea", true, null);
					System.out.println(new String(data, "utf-8"));
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			} else if (event.getState() == Watcher.Event.KeeperState.SyncConnected && event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
				System.out.println(event.getPath() + " the node children has changed!");
			}
			System.out.println(event.getType());
		});
	}

	@After
	public void close() throws InterruptedException {
		zk.close();
	}

	@Test
	public void testWatch() throws KeeperException, InterruptedException, UnsupportedEncodingException {
		byte[] data = zk.getData("/idea", true, null);
		System.out.println(new String(data, "UTF-8"));
		Thread.sleep(Long.MAX_VALUE);
	}
}
