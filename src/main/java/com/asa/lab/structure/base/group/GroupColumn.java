package com.asa.lab.structure.base.group;

import com.asa.lab.structure.base.summary.SummaryType;

import java.io.Serializable;

/**
 * @author andrew_asa
 * @date 2018/10/29.
 */
public class GroupColumn extends AbstractGroupSummaryColumn implements Serializable {

    /**
     * 默认为相同值为一组
     */
    private GroupType groupType = GroupType.ID;

    public GroupColumn() {

    }

    public GroupColumn(String source, GroupType groupType, String displayName) {

        setColumnSourceName(source);
        setDisplayName(displayName);
        this.groupType = groupType;
    }

    public GroupType getGroupType() {

        return groupType;
    }

    public void setGroupType(GroupType groupType) {

        this.groupType = groupType;
    }
}
