package com.asa.lab.internalimp.etl;

import com.asa.lab.internalimp.datasource.spark.SparkDataSource;
import com.asa.lab.internalimp.operator.DefaultETLOperatorJobBuilderContent;
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
 * @date 2018/10/9.
 */
public class DefaultETLBuilder implements ETLJobBuilder {

    @Override
    public DataSource build(ETLJobBuilderContent content, List<ETLOperator> ETLOperators) {


        Dataset<Row> dataSet = null;
        DefaultETLOperatorJobBuilderContent builderContent = DefaultETLOperatorJobBuilderContent.getInstance();
        for (ETLOperator operator : ETLOperators) {
            ETLOperatorJobBuilder jobBuilder = builderContent.getETLOperatorJobBuilder(operator);
            dataSet = jobBuilder.build(dataSet, operator, content);
        }
        return new SparkDataSource(dataSet);
    }
}
