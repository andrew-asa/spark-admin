package com.asa.lab.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/12.
 */
public class ArrayUtils {

    /**
     * array 数组转list
     *
     * @param elements
     * @param <T>
     * @return
     */
    public static <T> List<T> arrayToList(T[] elements) {

        ArrayList<T> result = new ArrayList<T>();
        if (elements != null) {
            for (T elem : elements) {
                result.add(elem);
            }
        }
        return result;
    }

    /**
     * array 数组转list
     *
     * @param elements
     * @param <T>
     * @return
     */
    public static <T> int length(T[] elements) {

        if (elements != null) {
            return elements.length;
        }
        return 0;
    }
}
