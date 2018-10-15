package com.asa.lab.internalimp.operator.select;


import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class SelectItem {

    private String tableName;

    private String columnName;

    public SelectItem(String tableName, String columnName) {

        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {

        return tableName;
    }

    public void setTableName(String tableName) {

        this.tableName = tableName;
    }

    public String getColumName() {

        return columnName;
    }

    public void setColumName(String columName) {

        this.columnName = columName;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof SelectItem)) {
            return false;
        }

        SelectItem that = (SelectItem) o;
        return AssistUtils.equals(tableName, that.tableName) &&
                AssistUtils.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(tableName, columnName);
    }
}
