package com.asa.lab.internalimp.operator.group;

import com.asa.lab.structure.base.group.GroupColumn;
import com.asa.lab.structure.base.group.SummaryColumn;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class GroupOperator implements ETLOperator {

    public static final String NAME = "group";

    /**
     * 分组项
     */
    private List<GroupColumn> groups;

    /**
     * 汇总项
     */
    private List<SummaryColumn> summarys;

    public List<GroupColumn> getGroups() {

        return groups;
    }

    public void setGroups(List<GroupColumn> groups) {

        this.groups = groups;
    }

    public List<SummaryColumn> getSummarys() {

        return summarys;
    }

    public void setSummarys(List<SummaryColumn> summarys) {

        this.summarys = summarys;
    }

    @Override
    public String getName() {

        return NAME;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupOperator)) {
            return false;
        }

        GroupOperator that = (GroupOperator) o;
        return AssistUtils.equals(groups, that.groups) &&
                AssistUtils.equals(summarys, that.summarys);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(groups, summarys);
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
