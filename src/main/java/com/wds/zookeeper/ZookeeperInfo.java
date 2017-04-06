package com.wds.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * Created by wangdongsong1229@163.com on 2017/4/6.
 */
public class ZookeeperInfo {

    private final String hostProt = "127.0.0.1:2181";
    private final static Logger LOGGER = LogManager.getLogger(ZookeeperTest.class);
    private boolean isLeader = false;
    private String serverId = Long.toString(new Random().nextLong());
    private ZooKeeper zooKeeper = null;

    public void startZookeeper() throws IOException {
        zooKeeper = new ZooKeeper(hostProt, 150000, (event) -> {
            LOGGER.info(event.getPath());
            LOGGER.info(event.toString());
        });
    }

    public void closeZookeeper() {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void runForMaster() throws InterruptedException {
        zooKeeper.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
        isLeader = true;
    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") + " ths leader");
        }
    };

    public boolean checkMaster() {
        while (true) {
            Stat stat = new Stat();
            try {
                byte[] data = zooKeeper.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException e) {
                return false;
            } catch (InterruptedException e) {

            }

            return false;
        }
    }
}
