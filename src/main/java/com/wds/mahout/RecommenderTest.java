package com.wds.mahout;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.model.DataModel;

import java.io.*;

/**
 * Created by wangdongsong1229@163.com on 2017/3/29.
 */
public class RecommenderTest {
    private static final String inputFile = "";
    private static final String outputFile = "";

    public static void main(String[] args) throws IOException {
        File ratingsFile = new File(outputFile);
        DataModel model = new FileDataModel(ratingsFile);

        //TODO
    }

    public static void createCSVRatingFile() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));

        String inputLine = null;
        String outputLine = null;
        String[] tmp = null;
        int i = 0;
        while ((inputLine = br.readLine()) != null && i < 1000) {
            i++;
            tmp = inputLine.split("::");
            outputLine = tmp[0] + "," + tmp[1];
            bw.write(outputLine);
            bw.newLine();
            bw.flush();
        }

        br.close();
        bw.close();
    }

}
