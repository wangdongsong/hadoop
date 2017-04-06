package com.wds.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * Created by wangdongsong1229@163.com on 2017/4/6.
 */
public class ZookeeperTest {

    public static void main(String[] args) throws Exception {
        ZookeeperInfo zookeeperInfo = new ZookeeperInfo();
        zookeeperInfo.startZookeeper();
        zookeeperInfo.runForMaster();
        zookeeperInfo.closeZookeeper();
    }
}
