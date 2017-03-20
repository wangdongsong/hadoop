package com.wds.hadoop.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.wds.hadoop.wordcount.WordCount.LOGGER;

/**
 * Created by wangdongsong1229@163.com on 2017/3/19.
 */
public class HDFSTest {
    private static final Logger LOGGER = LogManager.getLogger(HDFSTest.class);

    public static void main(String[] args) {
        uploadFile();
    }

    /**
     * 上传文件
     */
    private static void uploadFile() {
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        try {
            //conf.set("hadoop.home.dir", "F:\\Software\\hadoop-2.7.2\\hadoop-2.7.2");
            URI uri = new URI("hdfs://192.168.254.200:9000");

            FileSystem fileSystem = FileSystem.get(uri, conf);
            Path src = new Path("E:\\javaTest\\stackoverflow\\Comments.xml");
            Path dest = new Path("/user/hadoop/stackoverflow/data/");
            fileSystem.copyFromLocalFile(src, dest);

            fileSystem.close();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (URISyntaxException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
