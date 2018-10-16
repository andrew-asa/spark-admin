package com.asa.lab.internalimp.operator.add;

import com.asa.lab.internalimp.operator.add.expression.AddExpressionColumn;
import com.asa.lab.internalimp.operator.add.time.AddTimeDiffColumn;
import com.asa.lab.structure.base.time.TimeInterval;
import com.asa.lab.structure.datasource.Type;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddNewColumnOperatorHelper {

    public static AddExpressionColumn buildAddExpressionColumn(String columnName, String expression) {

        AddExpressionColumn column = new AddExpressionColumn(expression);
        column.setColumnName(columnName);
        return column;
    }

    public static AddTimeDiffColumn buildAddTimeDiffColumn(String columnName, String beginColumn, String endColumn, TimeInterval interval) {

        AddTimeDiffColumn addTimeDiffColumn = new AddTimeDiffColumn(beginColumn, endColumn, interval);
        addTimeDiffColumn.setColumnName(columnName);
        return addTimeDiffColumn;
    }

    public static AddTimeDiffColumn buildAddTimeDiffColumn(String columnName, String beginColumn, String endColumn, TimeInterval interval, Type type) {

        AddTimeDiffColumn addTimeDiffColumn = new AddTimeDiffColumn(beginColumn, endColumn, interval);
        addTimeDiffColumn.setColumnName(columnName);
        addTimeDiffColumn.setType(type);
        return addTimeDiffColumn;
    }
}
