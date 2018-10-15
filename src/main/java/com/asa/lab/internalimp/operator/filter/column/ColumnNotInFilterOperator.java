package com.asa.lab.internalimp.operator.filter.column;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 列不属于过滤
 */
public class ColumnNotInFilterOperator extends ColumnInFilterOperator {

    public static final String CONDITION_NAME = "COLUMN_NOT_IN";


    public ColumnNotInFilterOperator(String columnName, List<Object> value) {

        super(columnName, value);
    }

    @Override
    public String getConditionName() {

        return CONDITION_NAME;
    }
}
