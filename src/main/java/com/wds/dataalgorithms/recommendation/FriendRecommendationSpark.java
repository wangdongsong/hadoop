package com.wds.dataalgorithms.recommendation;

import java.util.List;
import java.util.Map;

/**
 * 好友推荐
 * Created by wangdongsong1229@163.com on 2017/9/1.
 */
public class FriendRecommendationSpark {

    public static void main(String[] args) {
        //Step2 处理输入参数

        //Step3 创建Spark上下文

        //Step4 读文件并创建RDD

        //Step5 实现map函数

        //Step6 实现reduce函数

        //Step7 生成所需要的最终输出
    }

    /**
     * 工具方法
     * @param mutualFriends
     * @return
     */
    private static String buildRecommendations(Map<Long, List<Long>> mutualFriends) {
        StringBuilder recommendations = new StringBuilder();
        mutualFriends.entrySet().stream().filter((entry) -> entry.getValue() != null).forEach((entry) ->{
            recommendations.append(entry.getKey());
            recommendations.append(" (");
            recommendations.append(entry.getValue().size());
            recommendations.append(":");
            recommendations.append(entry.getValue());
            recommendations.append("),");
        });
        return recommendations.toString();
    }

}
