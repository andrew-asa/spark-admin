package com.asa.lab.internalimp.operator.filter;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Function1;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 */
public class FilterJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        FilterETLOperator filterETLOperator = (FilterETLOperator) operator;
        if (filterETLOperator instanceof ColumnFilterETLOperator) {
            ColumnFilterETLOperator columnFilterETLOperator = (ColumnFilterETLOperator) filterETLOperator;
            String columnName = columnFilterETLOperator.getColumnName();
            List<Object> fv = columnFilterETLOperator.getValue();
            if (fv != null && fv.size() > 0) {
                Column column = dataSet.col(columnName);
                column = column.equalTo(fv.get(0));
                for (int i = 1; i < fv.size(); i++) {
                    column = column.or(dataSet.col(columnName).equalTo(fv.get(i)));
                }
                return dataSet.filter(column);
            }
        }
        return dataSet;
    }
}
