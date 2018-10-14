package com.asa.lab.utils;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 */
public class StringUtils {

    /**
     * 克隆
     * @param src
     * @param size
     * @return
     */
    public static String clone(String src, int size) {

        if (src != null) {
            StringBuffer sb = new StringBuffer();
            for(int i=0;i<size;i++) {
                sb.append(src);
            }
            return sb.toString();
        }
        return null;
    }
}
