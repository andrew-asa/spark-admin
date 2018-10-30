package com.asa.lab.structure.base.group.custom;

import com.asa.lab.structure.base.group.GroupColumn;
import com.asa.lab.structure.base.group.GroupType;
import com.asa.utils.AssistUtils;
import com.asa.utils.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/29.
 * 自定义分组
 */
public class CustomGroupColumn extends GroupColumn implements Serializable {

    private List<CustomGroupItem> custom;

    /***
     * 其他分组名字
     */
    private String otherGroupName;

    /**
     * 是否使用其他分组
     */
    private boolean userOther;

    public CustomGroupColumn() {

    }


    public CustomGroupColumn(String columnName, String displayName, List<CustomGroupItem> custom, boolean userOther, String otherGroupName) {

        super(columnName,GroupType.CUSTOM,displayName);
        this.custom = custom;
        this.otherGroupName = otherGroupName;
        this.userOther = userOther;
    }

    @Override
    public GroupType getGroupType() {

        return GroupType.CUSTOM;
    }

    public List<CustomGroupItem> getCustom() {

        return custom;
    }

    public void setCustom(List<CustomGroupItem> custom) {

        this.custom = custom;
    }

    public String getOtherGroupName() {

        return otherGroupName;
    }

    public void setOtherGroupName(String otherGroupName) {

        this.otherGroupName = otherGroupName;
    }

    public boolean isUserOther() {

        return userOther;
    }

    public void setUserOther(boolean userOther) {

        this.userOther = userOther;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof CustomGroupColumn)){
            return false;
        }

        CustomGroupColumn that = (CustomGroupColumn) o;
        return AssistUtils.equals(userOther, that.userOther) &&
                AssistUtils.equals(custom, that.custom) &&
                AssistUtils.equals(otherGroupName, that.otherGroupName);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(custom, otherGroupName, userOther);
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
