package com.asa.lab.internalimp.operator.add;

import com.asa.lab.internalimp.operator.filter.FilterDriver;
import com.asa.lab.internalimp.operator.filter.FilterDriverContent;
import com.asa.lab.internalimp.operator.filter.FilterOperator;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddNewColumnJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        AddNewColumnOperator addNewColumnOperator = (AddNewColumnOperator) operator;
        AddNewColumnDriver filterDriver = AddNewColumnDriverContent.getInstance().getAddNewColumnDriver(addNewColumnOperator);
        return filterDriver.build(dataSet, addNewColumnOperator, content);
    }
}
