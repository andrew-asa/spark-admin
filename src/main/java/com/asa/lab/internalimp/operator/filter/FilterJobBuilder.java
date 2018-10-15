package com.asa.lab.internalimp.operator.filter;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 */
public class FilterJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        FilterOperator filterOperator = (FilterOperator) operator;
        FilterDriver filterDriver = FilterDriverContent.getInstance().getFilterDriver(filterOperator);
        return filterDriver.build(dataSet, filterOperator, content);
    }
}
