package com.asa.lab.internalimp.operator.add;

import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 新增列
 */
public abstract class AddNewColumnOperator implements ETLOperator, AddNewColumnSubName {

    public static final String NAME = "addNewColumn";

    /**
     * 显示的名字列名
     */
    private String columnName;

    /**
     * 需要转换成为的类型
     */
    private Type type;

    /**
     * 是否是自动决定类型
     */
    private boolean typeAuto = true;

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

    public Type getType() {

        return type;
    }

    public void setType(Type type) {

        this.type = type;
    }

    public boolean isTypeAuto() {

        return typeAuto;
    }

    public void setTypeAuto(boolean typeAuto) {

        this.typeAuto = typeAuto;
    }
}
