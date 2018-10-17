package com.asa.lab.internalimp.operator.add.time;

import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.base.time.TimeGroup;
import com.asa.utils.AssistUtils;
import com.asa.utils.ComparatorUtils;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 */
public class AddTimeColumn extends AddNewColumnOperator {

    public static final String SUB_NAME = "addTimeColumn";

    public static final String SYSTEM_COLUMN_NAME = "";

    /**
     * 原始列名
     */
    private String orgColumnName;

    private TimeGroup group;

    public AddTimeColumn(String orgColumnName, TimeGroup group) {

        this.orgColumnName = orgColumnName;
        this.group = group;
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
    }

    public boolean isSystemColumn() {

        return ComparatorUtils.equals(orgColumnName, SYSTEM_COLUMN_NAME);
    }

    public String getOrgColumnName() {

        return orgColumnName;
    }

    public void setOrgColumnName(String orgColumnName) {

        this.orgColumnName = orgColumnName;
    }

    public TimeGroup getGroup() {

        return group;
    }

    public void setGroup(TimeGroup group) {

        this.group = group;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof AddTimeColumn)) {
            return false;
        }

        AddTimeColumn that = (AddTimeColumn) o;
        return AssistUtils.equals(orgColumnName, that.orgColumnName) &&
                AssistUtils.equals(group, that.group);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(orgColumnName, group);
    }
}
