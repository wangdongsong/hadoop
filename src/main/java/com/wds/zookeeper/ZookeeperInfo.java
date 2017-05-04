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

    private final String hostProt = "192.168.254.200:2181";
    private final static Logger LOGGER = LogManager.getLogger(ZookeeperTest.class);
    public boolean isLeader = false;
    private String serverId = Long.toString(new Random().nextLong());
    private ZooKeeper zooKeeper = null;

    private AsyncCallback.StringCallback createParentCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[])ctx);
                    break;
                case OK:
                    LOGGER.info( path + " Parent created");
                    break;
                case NODEEXISTS:
                    LOGGER.warn("Parent already registered: " + path);
                    break;
                default:
                    LOGGER.error("Something went wrong：" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    private AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
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
            System.out.println(Thread.currentThread().getName() + " -------I'm " + (isLeader ? "" : "not ") + " ths leader");
        }
    };

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
        while (true){
            zooKeeper.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
            isLeader = true;
            //break;

            if (checkMaster()) {
                break;
            }
        }
    }



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

    public void createParent(String path, byte[] data) {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallBack, data);
    }

    public void stopZk() throws InterruptedException {
        this.zooKeeper.close();
    }


}
