package com.wds.hadoop.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * 读取解析XML数据的工具类
 * 针对StackOverflow的数据结构（StackOverflow data dump）
 * <a>https://data.stackexchange.com/help</a>下载归档数据
 * Created by wangdongsong1229@163.com on 2017/3/13.
 */
public class MRDPUtils {
    private static final Logger LOGGER = LogManager.getLogger(MRDPUtils.class);

    public static Map<String, String> transFormXMLToMap(String xml) {
        Map<String, String> map = new HashMap<>();

        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");

            String key = null;
            String val = null;
            for (int i = 0; i < tokens.length - 1; i += 2) {
                key = tokens[i].trim();
                val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            LOGGER.error(e.getMessage(), e);
        }

        return map;
    }

    public static void main(String[] args) {
        String xml = "<row Id=\"1\" PostTypeId=\"1\" CreationDate=\"2013-12-18T20:25:25.003\" Score=\"30\" ViewCount=\"9495\" Body=\"&lt;p&gt;One of the Kindle Touch &lt;a href=&quot;http://kindleworld.blogspot.com/2013/03/kindle-news-kindle-touch-update-v5321.html&quot;&gt;updates&lt;/a&gt; included a &quot;Time-to-Read&quot; feature.  It seems to be based loosely on how fast I turn the page.  I usually turn back to the cover when I start reading a new book and flip back to the start of the introduction or first chapter.  So the status usually says &lt;code&gt;1 min left in chapter&lt;/code&gt; for a chapter or two.  I'm guessing the algorithm has determined that I have super-human reading speed.  Once I prove I don't, it becomes fairly accurate.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;But other times I sit on a page for several minutes when distracted by other things.  I would have assumed the calculation would show me taking much longer to finish chapters after doing that.  However, the reading time seems to be stable and fairly accurate after the initial weirdness.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;How does the algorithm work and why does each book have its own estimate?&lt;/p&gt;&#xA;\" OwnerUserId=\"3\" LastActivityDate=\"2013-12-19T04:18:10.800\" Title=\"How does the Kindle's reading rate algorithm work?\" Tags=\"&lt;kindle&gt;&lt;time-to-read&gt;&lt;kindle-touch&gt;\" AnswerCount=\"1\" CommentCount=\"0\" FavoriteCount=\"5\" />";
        //xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";
        xml = "<Post>";
        Map map = transFormXMLToMap(xml);
        LOGGER.info(map);
        LOGGER.info(LOGGER.getClass());
    }
}
