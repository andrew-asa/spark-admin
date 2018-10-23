package com.asa.lab.internalimp.operator.union;

import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 */
public class UnionColumnItem {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 列名 为空表示没有选择字段
     */
    private String columnName;

    public UnionColumnItem() {

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

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
