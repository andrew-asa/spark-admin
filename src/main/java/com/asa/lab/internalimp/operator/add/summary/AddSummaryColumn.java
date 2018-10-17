package com.asa.lab.internalimp.operator.add.summary;

import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.base.summary.SummaryType;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 新增求汇总列
 */
public class AddSummaryColumn extends AddNewColumnOperator {

    public static final String SUB_NAME = "addSummaryColumn";

    /**
     * 汇总列
     */
    private String summaryColumn;

    /**
     * 汇总方式
     */
    private SummaryType summaryType;

    /**
     * 分组列
     */
    private List<String> groupColumns;

    /**
     * 是否是组内求汇总值还是全部数据求汇总值
     */
    private boolean inGroup;

    public AddSummaryColumn(String summaryColumn, SummaryType summaryType, List<String> groupColumns, boolean inGroup) {

        this.summaryColumn = summaryColumn;
        this.summaryType = summaryType;
        this.groupColumns = groupColumns;
        this.inGroup = inGroup;
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
    }

    public SummaryType getSummaryType() {

        return summaryType;
    }

    public void setSummaryType(SummaryType summaryType) {

        this.summaryType = summaryType;
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

    public String getSummaryColumn() {

        return summaryColumn;
    }

    public void setSummaryColumn(String summaryColumn) {

        this.summaryColumn = summaryColumn;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof AddSummaryColumn)) {
            return false;
        }

        AddSummaryColumn that = (AddSummaryColumn) o;
        return AssistUtils.equals(inGroup, that.inGroup) &&
                AssistUtils.equals(summaryColumn, that.summaryColumn) &&
                AssistUtils.equals(summaryType, that.summaryType) &&
                AssistUtils.equals(groupColumns, that.groupColumns);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(summaryColumn, summaryType, groupColumns, inGroup);
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this,
                                    "summaryColumn",
                                    "summaryType",
                                    "groupColumns",
                                    "inGroup"
        );
    }
}
