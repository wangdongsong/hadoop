package com.wds.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by wangdongsong1229@163.com on 2017/4/6.
 */
public class ZookeeperTest {

    private static final String hostProt = "127.0.0.1:2181";
    private final static Logger LOGGER = LogManager.getLogger(ZookeeperTest.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper(hostProt, 150000, (event) -> {
            LOGGER.info(event.getPath());
            LOGGER.info(event.toString());
        });

        Thread.sleep(50000);
    }
}
