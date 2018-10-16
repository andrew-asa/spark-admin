package com.asa.lab.structure.base.time;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 时间间隔
 */
public enum TimeInterval {
    Year("year"),
    Season("season"),
    Month("month"),
    Week("week"),
    Day("day"),
    Hour("hour"),
    Minute("minute"),
    Second("second");

    String name;

    TimeInterval(String name) {

        this.name = name;
    }

    @Override
    public String toString() {

        return "TimeInterval{" +
                "name='" + name + '\'' +
                '}';
    }
}
