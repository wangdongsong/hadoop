package com.wds.dataalgorithms.mba;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 给定一个交易T={I1, I2, I3...In},使用该类生成有序项集的所有组合
 * <p>
 * Created by wangdongsong1229@163.com on 2017/8/27.
 */
public class Combination {

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
        List<List<T>> result = new ArrayList<List<T>>();
        for(int i = 0; i < elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }
        return result;
    }

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<List<T>>();
        for(int i = 0; i < elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }
        return result;
    }

    public static <T extends Comparable<? super T>> List<List<T>> findSourtedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<>();
        if (n == 0) {
            result.add(new ArrayList<>());
            return result;
        }

        List<List<T>> combinations = findSortedCombinations(elements, n - 1);

        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }

                List<T> list = new ArrayList<>();
                list.addAll(combination);
                if (list.contains(element)) {
                    continue;
                }

                list.add(element);
                Collections.sort(list);
                if (result.contains(list)) {
                    continue;
                }
                result.add(list);
            }
        }
        return result;
    }

}
