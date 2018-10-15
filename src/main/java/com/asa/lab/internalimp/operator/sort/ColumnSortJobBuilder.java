package com.asa.lab.internalimp.operator.sort;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.utils.ListUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;


/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class ColumnSortJobBuilder implements ETLOperatorJobBuilder {


    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        ColumnSortOperator sortOperator = (ColumnSortOperator) operator;
        List<SortItem> sortItems = sortOperator.getSortList();
        List<Column> sortColumn = new ArrayList<>();
        if (ListUtils.isNotEmpty(sortItems)) {
            for (SortItem sortItem : sortItems) {
                String columnName = sortItem.getColumnName();
                Column column;
                if (sortItem.isDesc()) {
                    column = dataSet.col(columnName).desc();
                } else {
                    column = dataSet.col(columnName).asc();
                }
                sortColumn.add(column);
            }
            dataSet = dataSet.sort(sortColumn.toArray(new Column[0]));
        }
        return dataSet;
    }
}
