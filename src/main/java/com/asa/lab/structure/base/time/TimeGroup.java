package com.asa.lab.structure.base.time;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 时间分组
 */
public enum TimeGroup {
    /**
     * 年
     */
    Year("Year"),
    /**
     * 季节
     */
    Season("Season"),
    /**
     * 月
     */
    Month("Month"),
    /**
     * 一年中第几周
     */
    Week("Week"),
    /**
     * 一个月的第几天
     */
    Day("Day"),
    /**
     * 小时值
     */
    Hour("Hour"),
    /**
     * 分钟值
     */
    Minute("Minute"),
    /**
     * 秒数值
     */
    Second("Second"),
    /**
     * 一周的第几天
     */
    WeekDay("WeekDay"),
    /**
     * 年-季度 例:2012-03
     */
    YearSeason("YearSeason"),
    /**
     * 年-月 例:2012-02
     */
    YearMonth("YearMonth"),
    /**
     * 年-周 列: 2012-53
     */
    YearWeek("YearWeek"),
    /**
     * 年-月-日
     */
    YearMonthDay("YearMonthDay"),
    /**
     * 年-月-日-时
     */
    YearMonthDayHour("YearMonthDayHour"),
    /**
     * 年-月-日-时-分
     */
    YearMonthDayHourMinute("YearMonthDayHourMinute"),
    /**
     * 年-月-日-时-秒
     */
    YearMonthDayHourMinuteSecond("YearMonthDayHourMinuteSecond");

    private String name;

    TimeGroup(String name) {

        this.name = name;
    }

    @Override
    public String toString() {

        return "TimeGroup{" +
                "name='" + name + '\'' +
                '}';
    }
}
