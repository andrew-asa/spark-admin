package com.asa.lab.internalimp.operator.add;

import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 新增列
 */
public abstract class AddNewColumnOperator implements ETLOperator, AddNewColumnSubName {

    public static final String NAME = "addNewColumn";

    /**
     * 列名
     */
    private String columnName;

    @Override
    public String getName() {

        return NAME;
    }

    public String getColumnName() {

        return columnName;
    }

    public void setColumnName(String columnName) {

        this.columnName = columnName;
    }
}
