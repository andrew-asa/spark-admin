package com.asa.lab.internalimp.operator.select;

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
public class SelectJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        SelectOperator selectOperator = (SelectOperator) operator;
        List<SelectItem> selectItems = selectOperator.getItems();
        List<Column> scs = new ArrayList<>();
        if (ListUtils.isNotEmpty(selectItems)) {
            for (SelectItem item : selectItems) {
                Column column = dataSet.col(item.getColumName());
                scs.add(column);
            }
            dataSet = dataSet.select(scs.toArray(new Column[0]));
        }
        return dataSet;
    }
}
