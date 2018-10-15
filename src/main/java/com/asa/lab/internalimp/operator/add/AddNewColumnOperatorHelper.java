package com.asa.lab.internalimp.operator.add;

import com.asa.lab.internalimp.operator.add.expression.AddExpressionColumn;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddNewColumnOperatorHelper {

    public static AddExpressionColumn buildAddExpressionColumn(String columnName,String expression) {

        AddExpressionColumn column = new AddExpressionColumn(expression);
        column.setColumnName(columnName);
        return column;
    }
}
