package com.asa.lab.structure.service.spark;

import com.asa.lab.expection.DSUnSupportException;
import com.asa.lab.structure.base.time.TimeGroup;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.stream.Collectors;


/**
 * @author andrew_asa
 * @date 2018/10/16.
 */
public class SparkFunctionsHelper {

    public static final String DATE_SEPARATOR = "-";

    public static final String ROW_INDEX_COLUMN_NAME = "__row_index_column_name__";

    /**
     * 获取季节
     *
     * @param columnName 列名
     * @param dataset    dataFrame
     * @return
     */
    public static Column season(String columnName, Dataset dataset) {

        return functions.ceil(functions.month(dataset.col(columnName)).divide(3));
    }

    /***
     * 获取季节
     * @param column 列
     * @return
     */
    public static Column season(Column column) {

        return functions.ceil(functions.month(column).divide(3));
    }

    /**
     * 时间分组
     *
     * @param column 时间列
     * @param group  分组类型
     * @return
     */
    public static Column timeColumn(Column column, TimeGroup group) {

        switch (group) {
            case Year:
                return functions.year(column);
            case Season:
                return season(column);
            case Month:
                return functions.month(column);
            case Week:
                return functions.weekofyear(column);
            case Day:
                return functions.dayofmonth(column);
            case Hour:
                return functions.hour(column);
            case Minute:
                return functions.minute(column);
            case Second:
                return functions.second(column);
            case WeekDay:
                return functions.dayofweek(column);
            case YearSeason:
                return functions.concat_ws(DATE_SEPARATOR,
                                           functions.year(column),
                                           season(column));
            case YearMonth:
                return functions.concat_ws(DATE_SEPARATOR,
                                           functions.year(column),
                                           completeWithZero(functions.month(column)));
            case YearWeek:
                return functions.concat_ws(DATE_SEPARATOR,
                                           functions.year(column),
                                           completeWithZero(functions.weekofyear(column)));
            case YearMonthDay:
                return functions.concat_ws(DATE_SEPARATOR,
                                           functions.year(column),
                                           completeWithZero(functions.month(column)),
                                           completeWithZero(functions.dayofmonth(column)));
            case YearMonthDayHour:
                return functions.concat_ws(DATE_SEPARATOR,
                                           functions.year(column),
                                           completeWithZero(functions.month(column)),
                                           completeWithZero(functions.dayofmonth(column)),
                                           completeWithZero(functions.hour(column)));
            case YearMonthDayHourMinute:
                return functions.concat_ws(DATE_SEPARATOR,
                                           functions.year(column),
                                           completeWithZero(functions.month(column)),
                                           completeWithZero(functions.dayofmonth(column)),
                                           completeWithZero(functions.hour(column)),
                                           completeWithZero(functions.minute(column)));
            case YearMonthDayHourMinuteSecond:
                return functions.concat_ws(DATE_SEPARATOR,
                                           functions.year(column),
                                           completeWithZero(functions.month(column)),
                                           completeWithZero(functions.dayofmonth(column)),
                                           completeWithZero(functions.hour(column)),
                                           completeWithZero(functions.minute(column)),
                                           completeWithZero(functions.second(column)));
            default:
                throw new DSUnSupportException("un support group {}", group);
        }
    }

    /**
     * 补全零
     * 1  -> 01
     * 12 -> 12
     *
     * @param column
     * @return
     */
    private static Column completeWithZero(Column column) {

        return functions.format_string("%02d", column);
    }

    /**
     * 所有值求和
     *
     * @param summaryColumnName
     * @return
     */
    public static Column sumInAll(String summaryColumnName) {

        return functions.sum(summaryColumnName).over();
    }

    /**
     * 所有行最小值
     *
     * @param summaryColumnName
     * @return
     */
    public static Column minInAll(String summaryColumnName) {

        return functions.min(summaryColumnName).over();
    }

    /**
     * 所有行最大值
     *
     * @param summaryColumnName
     * @return
     */
    public static Column maxInAll(String summaryColumnName) {

        return functions.max(summaryColumnName).over();
    }

    /**
     * 所有行求平均
     *
     * @param summaryColumnName
     * @return
     */
    public static Column avgInAll(String summaryColumnName) {

        return functions.avg(summaryColumnName).over();
    }

    /**
     * 组内所有值求和
     *
     * @param summaryColName    求和列
     * @param groupsColumnNames 分组列
     * @return
     */
    public static Column sumInGroup(String summaryColName, List<String> groupsColumnNames) {

        return functions.sum(summaryColName)
                .over(Window.partitionBy(transferToSeqOfColumns(groupsColumnNames)));
    }

    /**
     * 组内所有值求最大值
     *
     * @param summaryColumnName 求和列
     * @param groupsColumnNames 分组列
     * @return
     */
    public static Column maxInGroup(String summaryColumnName, List<String> groupsColumnNames) {

        return functions.max(summaryColumnName)
                .over(Window.partitionBy(transferToSeqOfColumns(groupsColumnNames)));
    }

    /**
     * 组内所有值求最小值
     *
     * @param summaryColName    求和列
     * @param groupsColumnNames 分组列
     * @return
     */
    public static Column minInGroup(String summaryColName, List<String> groupsColumnNames) {

        return functions.min(summaryColName)
                .over(Window.partitionBy(transferToSeqOfColumns(groupsColumnNames)));
    }

    /**
     * 组内所有值求最平均
     *
     * @param summaryColName    求和列
     * @param groupsColumnNames 分组列
     * @return
     */
    public static Column avgInGroup(String summaryColName, List<String> groupsColumnNames) {

        return functions.avg(summaryColName)
                .over(Window.partitionBy(transferToSeqOfColumns(groupsColumnNames)));
    }

    /**
     * 所有值累计
     *
     * @param summaryColName
     * @return
     */
    public static Column cumulativeInAll(String summaryColName) {

        return functions.sum(summaryColName)
                .over(Window.rowsBetween(Window.unboundedPreceding(), Window.currentRow()));
    }

    /**
     * 组内所有值累计
     *
     * @param summaryColName
     * @return
     */
    public static Column cumulativeInGroup(String summaryColName, List<String> groupColumns) {

        return functions.sum(summaryColName)
                .over(
                        Window.partitionBy(transferToSeqOfColumns(groupColumns))
                                .rowsBetween(Window.unboundedPreceding(), Window.currentRow())
                );
    }

    public static Seq<Column> transferToSeqOfColumns(List<String> fieldList) {

        List<Column> columnList = fieldList.stream().map(Column::new).collect(Collectors.toList());
        return JavaConverters.asScalaIteratorConverter(columnList.iterator()).asScala().toSeq();
    }
}
