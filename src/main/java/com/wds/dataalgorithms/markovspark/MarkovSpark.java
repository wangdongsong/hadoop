package com.wds.dataalgorithms.markovspark;

import java.io.Serializable;

/**
 * Created by wangdongsong1229@163.com on 2017/9/4.
 */
public class MarkovSpark implements Serializable {

    public static void main(String[] args) {
        //Step1 处理输入参数
        if (args.length != 1) {
            System.err.println("Usage: MarkovSpark <input-path>");
            System.exit(0);
        }

        final String inputPath = args[0];
        System.out.println("inputPath:args[0]=" + args[0]);

    }

}
