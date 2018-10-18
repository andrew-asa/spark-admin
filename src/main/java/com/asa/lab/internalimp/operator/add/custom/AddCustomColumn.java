package com.asa.lab.internalimp.operator.add.custom;

import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 新增求自定义分组
 */
public class AddCustomColumn extends AddNewColumnOperator {

    public static final String SUB_NAME = "addCustomColumn";

    private List<CustomGroupItem> custom;

    /***
     * 其他分组名字
     */
    private String otherGroupName;

    /**
     * 是否使用其他分组
     */
    private boolean userOther;

    /**
     * 定制列
     */
    private String customColumn;

    public AddCustomColumn(String customColumn, List<CustomGroupItem> custom, boolean userOther, String otherGroupName) {

        this.customColumn = customColumn;
        this.custom = custom;
        this.userOther = userOther;
        this.otherGroupName = otherGroupName;
    }

    public List<CustomGroupItem> getCustom() {

        return custom;
    }

    public void setCustom(List<CustomGroupItem> custom) {

        this.custom = custom;
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
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

    public String getCustomColumn() {

        return customColumn;
    }

    public void setCustomColumn(String customColumn) {

        this.customColumn = customColumn;
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
        return AssistUtils.equals(userOther, that.userOther) &&
                AssistUtils.equals(custom, that.custom) &&
                AssistUtils.equals(otherGroupName, that.otherGroupName);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(custom, otherGroupName, userOther);
    }
}
