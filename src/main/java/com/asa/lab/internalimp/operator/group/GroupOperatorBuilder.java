package com.asa.lab.internalimp.operator.group;

import com.asa.lab.structure.base.group.GroupColumn;
import com.asa.lab.structure.base.group.GroupType;
import com.asa.lab.structure.base.group.SummaryColumn;
import com.asa.lab.structure.base.group.custom.CustomGroupColumn;
import com.asa.lab.structure.base.summary.SummaryType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/29.
 */
public class GroupOperatorBuilder {

    /**
     * 分组项
     */
    private List<GroupColumn> groups;

    /**
     * 汇总项
     */
    private List<SummaryColumn> summarys;

    private CustomGroupColumn customGroupColumn;

    private GroupColumn IDGroup;

    public GroupOperatorBuilder() {

        groups = new ArrayList<>();
        summarys = new ArrayList<>();
    }

    public GroupOperatorBuilder addIDGroup(String source, GroupType groupType, String displayName) {

        IDGroup = new GroupColumn(source, groupType, displayName);
        groups.add(IDGroup);
        return this;
    }

    public GroupOperatorBuilder startAddCustomGroup() {

        customGroupColumn = new CustomGroupColumn();
        return this;
    }

    public GroupOperatorBuilder setGroupSourceColumn(String columnName) {

        if (customGroupColumn != null) {
            customGroupColumn.setColumnSourceName(columnName);
        }
        return this;
    }

    public GroupOperatorBuilder setGroupDisplayName(String columnName) {

        if (customGroupColumn != null) {
            customGroupColumn.setDisplayName(columnName);
        }
        return this;
    }

    public GroupOperatorBuilder endAddCustomGroup() {

        groups.add(customGroupColumn);
        customGroupColumn = null;
        return this;
    }

    public GroupOperatorBuilder addSummaryColumn(String source, SummaryType summaryType, String displayName) {

        SummaryColumn summaryColumn = new SummaryColumn(source, summaryType, displayName);
        summarys.add(summaryColumn);
        return this;
    }

    public GroupOperator build() {

        GroupOperator operator = new GroupOperator();
        operator.setGroups(groups);
        operator.setSummarys(summarys);
        return operator;
    }
}
