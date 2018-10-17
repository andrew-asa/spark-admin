package com.asa.lab.internalimp.operator.add;


import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 */
public abstract class AbstractAddSummaryColumn extends AddNewColumnOperator {

    /**
     * 汇总列
     */
    private String summaryColumn;

    /**
     * 分组列
     */
    private List<String> groupColumns;

    /**
     * 是否是组内求汇总值还是全部数据求汇总值
     */
    private boolean inGroup;

    public AbstractAddSummaryColumn(String summaryColumn, List<String> groupColumns, boolean inGroup) {

        this.summaryColumn = summaryColumn;
        this.groupColumns = groupColumns;
        this.inGroup = inGroup;
    }

    public String getSummaryColumn() {

        return summaryColumn;
    }

    public void setSummaryColumn(String summaryColumn) {

        this.summaryColumn = summaryColumn;
    }

    public List<String> getGroupColumns() {

        return groupColumns;
    }

    public void setGroupColumns(List<String> groupColumns) {

        this.groupColumns = groupColumns;
    }

    public boolean isInGroup() {

        return inGroup;
    }

    public void setInGroup(boolean inGroup) {

        this.inGroup = inGroup;
    }
}
