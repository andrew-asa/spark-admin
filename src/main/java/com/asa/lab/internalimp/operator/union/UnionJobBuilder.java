package com.asa.lab.internalimp.operator.union;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class UnionJobBuilder implements ETLOperatorJobBuilder{

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        return dataSet;
    }
}
