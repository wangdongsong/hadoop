package com.wds.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Random;

/**
 * Created by wangdongsong1229@163.com on 2017/5/6.
 */
public class ZookeeperWorker implements Watcher {
    private final static Logger LOGGER = LogManager.getLogger(ZookeeperWorker.class);
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString(new Random().nextInt());

    public ZookeeperWorker(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        LOGGER.info(event.toString() + ", " + hostPort);
    }

    public void register() {
        zk.create("/workers/worker-" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createrWorkerCallback, null);
    }

    private AsyncCallback.StringCallback createrWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    LOGGER.info("Registered successfully: " + serverId);
                    break;
                case NODEEXISTS:
                    LOGGER.warn("Already regsitered:" + serverId);
                    break;
                default:
                    LOGGER.error("Something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        String hostPort = "";
        ZookeeperWorker worker = new ZookeeperWorker(hostPort);
        worker.startZK();
        worker.register();
        Thread.sleep(30000);
    }
}
