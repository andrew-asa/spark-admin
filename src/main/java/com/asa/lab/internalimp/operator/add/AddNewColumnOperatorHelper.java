package com.asa.lab.internalimp.operator.add;

import com.asa.lab.internalimp.operator.add.cumulative.AddCumulativeColumn;
import com.asa.lab.internalimp.operator.add.expression.AddExpressionColumn;
import com.asa.lab.internalimp.operator.add.rank.AddRankColumn;
import com.asa.lab.internalimp.operator.add.summary.AddSummaryColumn;
import com.asa.lab.internalimp.operator.add.time.AddTimeColumn;
import com.asa.lab.internalimp.operator.add.time.AddTimeDiffColumn;
import com.asa.lab.structure.base.summary.SummaryType;
import com.asa.lab.structure.base.time.TimeGroup;
import com.asa.lab.structure.base.time.TimeInterval;
import com.asa.lab.structure.datasource.Type;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddNewColumnOperatorHelper {

    /**
     * 新增表达式列
     *
     * @param columnName
     * @param expression
     * @return
     */
    public static AddExpressionColumn buildAddExpressionColumn(String columnName, String expression) {

        AddExpressionColumn column = new AddExpressionColumn(expression);
        column.setColumnName(columnName);
        return column;
    }

    /**
     * 新增时间差列
     *
     * @param columnName
     * @param beginColumn
     * @param endColumn
     * @param interval
     * @return
     */
    public static AddTimeDiffColumn buildAddTimeDiffColumn(String columnName, String beginColumn, String endColumn, TimeInterval interval) {

        AddTimeDiffColumn addTimeDiffColumn = new AddTimeDiffColumn(beginColumn, endColumn, interval);
        addTimeDiffColumn.setColumnName(columnName);
        return addTimeDiffColumn;
    }

    /**
     * 新增时间差列
     *
     * @param columnName
     * @param beginColumn
     * @param endColumn
     * @param interval
     * @param type
     * @return
     */
    public static AddTimeDiffColumn buildAddTimeDiffColumn(String columnName, String beginColumn, String endColumn, TimeInterval interval, Type type) {

        AddTimeDiffColumn addTimeDiffColumn = new AddTimeDiffColumn(beginColumn, endColumn, interval);
        addTimeDiffColumn.setColumnName(columnName);
        addTimeDiffColumn.setType(type);
        return addTimeDiffColumn;
    }

    /**
     * 添加时间列
     *
     * @param columnName
     * @param orgColName
     * @param group
     * @param type
     * @return
     */
    public static AddTimeColumn buildAddTimeColumn(String columnName, String orgColName, TimeGroup group, Type type) {

        AddTimeColumn addTimeDiffColumn = new AddTimeColumn(orgColName, group);
        addTimeDiffColumn.setColumnName(columnName);
        addTimeDiffColumn.setType(type);
        return addTimeDiffColumn;
    }

    /**
     * 新增汇总列
     *
     * @param columnName
     * @param summaryColumnName
     * @param summaryType
     * @param groupColumns
     * @param inGroup
     * @return
     */
    public static AddSummaryColumn buildAddSummaryColumn(String columnName, String summaryColumnName, SummaryType summaryType, List<String> groupColumns, boolean inGroup) {

        AddSummaryColumn addTimeDiffColumn = new AddSummaryColumn(summaryColumnName, summaryType, groupColumns, inGroup);
        addTimeDiffColumn.setColumnName(columnName);
        return addTimeDiffColumn;
    }

    /**
     * 新增列--累计值
     *
     * @param columnName
     * @param summaryColumnName
     * @param groupColumns
     * @param inGroup
     * @return
     */
    public static AddCumulativeColumn buildAddCumulativeColumn(String columnName, String summaryColumnName, List<String> groupColumns, boolean inGroup) {

        AddCumulativeColumn cumulativeColumn = new AddCumulativeColumn(summaryColumnName, groupColumns, inGroup);
        cumulativeColumn.setColumnName(columnName);
        return cumulativeColumn;
    }

    /**
     * 新增列--排名
     *
     * @param columnName
     * @param summaryColumnName
     * @param groupColumns
     * @param inGroup
     * @return
     */
    public static AddRankColumn buildAddRankCumulativeColumn(String columnName, String summaryColumnName, List<String> groupColumns, boolean inGroup,boolean desc) {

        AddRankColumn rankColumn = new AddRankColumn(summaryColumnName, groupColumns, inGroup,desc);
        rankColumn.setColumnName(columnName);
        return rankColumn;
    }
}
