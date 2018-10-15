package com.asa.lab.internalimp.operator.filter.column;

import com.asa.lab.internalimp.operator.filter.FilterDriver;
import com.asa.lab.internalimp.operator.filter.FilterOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class ColumnInFilterDriver implements FilterDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, FilterOperator operator, ETLJobBuilderContent content) {

        ColumnInFilterOperator columnFilterETLOperator = (ColumnInFilterOperator) operator;
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
        return dataSet;
    }
}
