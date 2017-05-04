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
        new Thread(() -> {
            try {
                electLeader();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                electLeader();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

    }

    private static void electLeader() throws IOException, InterruptedException {
        ZookeeperInfo zookeeperInfo = new ZookeeperInfo();
        zookeeperInfo.startZookeeper();
        //Thread.sleep(60000);
        zookeeperInfo.runForMaster();

        if (zookeeperInfo.isLeader) {
            System.out.println(Thread.currentThread().getName() + " I am leader");
            Thread.sleep(60000);
        } else {
            System.out.println(Thread.currentThread().getName() + " I am not leader");
        }

        zookeeperInfo.stopZk();
        //Thread.sleep(5000);
        //zookeeperInfo.closeZookeeper();
    }
}
