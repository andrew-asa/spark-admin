package com.asa.lab.structure.base.group;


import java.io.Serializable;

/**
 * 抽象分组汇总列
 *
 * @author andrew_asa
 * @date 2018/10/29.
 */
public class AbstractGroupSummaryColumn implements ColumnSource, Serializable {

    private String columnSourceName;

    private String displayName;

    @Override
    public String getColumnSourceName() {

        return columnSourceName;
    }

    public void setColumnSourceName(String columnSourceName) {

        this.columnSourceName = columnSourceName;
    }

    @Override
    public String getDisplayName() {

        return displayName;
    }

    public void setDisplayName(String displayName) {

        this.displayName = displayName;
    }
}
