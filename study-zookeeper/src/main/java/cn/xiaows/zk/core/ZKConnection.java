package cn.xiaows.zk.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ZKConnection {
    protected String hosts = "172.18.1.5:2181,172.18.1.6:2181,172.18.1.7:2181";
    private static final int SESSION_TIMEOUT = 6000;
    private CountDownLatch connectedSingal = new CountDownLatch(1);
    protected ZooKeeper zk = null;

    public void connect() throws Exception {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 连接建立,回调process接口时,其event.getState()为KeeperState.SyncConnected
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    // 放开闸门,wait在connect方法上的线程将被唤醒
                    connectedSingal.countDown();
                }
            }
        });
    }

    /**
     * 创建临时节点
     */
    public void create(String nodePath, byte[] data) throws Exception {
        zk.create(nodePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
}
