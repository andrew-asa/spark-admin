package com.asa.lab.internalimp.operator.add.custom;

import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.base.group.GroupColumn;
import com.asa.utils.AssistUtils;

import java.io.Serializable;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 新增求自定义分组
 */
public class AddCustomColumn extends AddNewColumnOperator implements Serializable {

    public static final String SUB_NAME = "addCustomColumn";

    /**
     * 分组信息
     */
    private GroupColumn groupInfo;

    public AddCustomColumn(GroupColumn groupInfo) {

        this.groupInfo = groupInfo;
    }

    public GroupColumn getGroupInfo() {

        return groupInfo;
    }

    public void setGroupInfo(GroupColumn groupInfo) {

        this.groupInfo = groupInfo;
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof AddCustomColumn)) {
            return false;
        }

        AddCustomColumn that = (AddCustomColumn) o;
        return AssistUtils.equals(groupInfo, that.groupInfo);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(groupInfo);
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
