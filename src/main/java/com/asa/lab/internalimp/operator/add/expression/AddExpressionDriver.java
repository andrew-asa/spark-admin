package com.asa.lab.internalimp.operator.add.expression;

import com.asa.lab.internalimp.operator.add.AddNewColumnDriver;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.utils.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddExpressionDriver implements AddNewColumnDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, AddNewColumnOperator operator, ETLJobBuilderContent content) {

        AddExpressionColumn expressionColumn = (AddExpressionColumn) operator;
        String expression = expressionColumn.getExpression();
        if (StringUtils.isNotEmpty(expression)) {
            String columnName = expressionColumn.getColumnName();
            dataSet=dataSet.selectExpr("*", expression + " as " + columnName);
        }
        return dataSet;
    }
}
