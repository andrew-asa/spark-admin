package com.asa.lab.utils;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 */
public class ComparatorUtils {

    public static boolean equals(Object var0, Object var1) {

        return var0 == null && var1 == null ? true : (var0 == null ? false : (var1 == null ? false : var0.equals(var1)));
    }
}
