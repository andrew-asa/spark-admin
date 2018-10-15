package com.asa.lab.internalimp.operator.join;

import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class JoinJobBuilder implements ETLOperatorJobBuilder {


    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        return dataSet;
    }
}
