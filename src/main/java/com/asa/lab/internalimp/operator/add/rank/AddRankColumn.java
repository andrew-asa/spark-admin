package com.asa.lab.internalimp.operator.add.rank;

import com.asa.lab.internalimp.operator.add.AbstractAddSummaryColumn;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 新增求汇总列-排名
 */
public class AddRankColumn extends AbstractAddSummaryColumn {

    public static final String SUB_NAME = "addRankColumn";

    /**
     * 是否降序
     */
    private boolean desc;

    public AddRankColumn(String summaryColumn, List<String> groupColumns, boolean inGroup, boolean desc) {

        super(summaryColumn, groupColumns, inGroup);
        this.desc = desc;
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
    }

    public boolean isDesc() {

        return desc;
    }

    public void setDesc(boolean desc) {

        this.desc = desc;
    }

    @Override
    public boolean equals(Object o) {


        if (this == o) {
            return true;
        }
        if (!(o instanceof AddRankColumn)) {
            return false;
        }

        AddRankColumn that = (AddRankColumn) o;
        return AssistUtils.equals(desc, that.desc) && super.equals(o);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(desc);
    }
}
