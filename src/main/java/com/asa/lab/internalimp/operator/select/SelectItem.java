package com.asa.lab.internalimp.operator.select;


import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class SelectItem {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 列名
     */
    private String columnName;

    /**
     * 显示的名字
     */
    private String name;

    public SelectItem(String tableName, String columnName, String name) {

        this.tableName = tableName;
        this.columnName = columnName;
        this.name = name;
    }

    public String getTableName() {

        return tableName;
    }

    public void setTableName(String tableName) {

        this.tableName = tableName;
    }

    public String getColumnName() {

        return columnName;
    }

    public void setColumnName(String columnName) {

        this.columnName = columnName;
    }

    public String getName() {

        return name;
    }

    public void setName(String name) {

        this.name = name;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof SelectItem)) {
            return false;
        }

        SelectItem item = (SelectItem) o;
        return AssistUtils.equals(tableName, item.tableName) &&
                AssistUtils.equals(columnName, item.columnName) &&
                AssistUtils.equals(name, item.name);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(tableName, columnName, name);
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
