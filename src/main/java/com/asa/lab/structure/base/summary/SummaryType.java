package com.asa.lab.structure.base.summary;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 汇总方式
 */
public enum SummaryType {

    /**
     * 求和
     */
    sum("sum"),
    /**
     * 求最大值
     */
    max("max"),
    /**
     * 求最小值
     */
    min("min"),
    /**
     * 求平均
     */
    avg("avg");

    private String name;

    SummaryType(String name) {

        this.name = name;
    }

    @Override
    public String toString() {

        return "TimeGroup{" +
                "name='" + name + '\'' +
                '}';
    }
}
