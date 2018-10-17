package com.asa.lab.internalimp.operator.add.summary;

import com.asa.lab.internalimp.operator.add.AbstractAddSummaryColumn;
import com.asa.lab.structure.base.summary.SummaryType;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 新增求汇总列
 */
public class AddSummaryColumn extends AbstractAddSummaryColumn {

    public static final String SUB_NAME = "addSummaryColumn";

    /**
     * 汇总方式
     */
    private SummaryType summaryType;


    public AddSummaryColumn(String summaryColumn, SummaryType summaryType, List<String> groupColumns, boolean inGroup) {

        super(summaryColumn, groupColumns, inGroup);
        this.summaryType = summaryType;
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

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof AddSummaryColumn)) {
            return false;
        }

        AddSummaryColumn that = (AddSummaryColumn) o;
        return AssistUtils.equals(summaryType, that.summaryType);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(summaryType);
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
