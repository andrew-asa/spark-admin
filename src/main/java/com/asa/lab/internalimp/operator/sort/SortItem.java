package com.asa.lab.internalimp.operator.sort;

import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 排序项
 */
public class SortItem {

    private String columnName;

    private boolean desc;

    public SortItem(String columnName, boolean desc) {

        this.columnName = columnName;
        this.desc = desc;
    }

    public String getColumnName() {

        return columnName;
    }

    public void setColumnName(String columnName) {

        this.columnName = columnName;
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
        if (!(o instanceof SortItem)) {
            return false;
        }

        SortItem sortItem = (SortItem) o;
        return AssistUtils.equals(desc, sortItem.desc) &&
                AssistUtils.equals(columnName, sortItem.columnName);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(columnName, desc);
    }
}
