package com.asa.lab.internalimp.etl;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.spark.SparkDataSource;
import com.asa.lab.internalimp.operator.DefaultETLOperatorJobBuilderContent;
import com.asa.lab.internalimp.sql.DSConstant;
import com.asa.lab.internalimp.sql.DSRelationProvider;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class DefaultETLBuilder implements ETLJobBuilder {

    @Override
    public DataSource build(DataSource source, ETLJobBuilderContent content, List<ETLOperator> ETLOperators) {

        SparkSession sparkSession = SparkContentManager.getInstance().getSession();
        DataSourceHelper.makeSureDataSourceHaveName(source);
        DataBaseContent.getInstance().addDataSource(source.getName(), source);
        Dataset<Row> dataSet = sparkSession
                .read()
                .format(DSRelationProvider.CLASSPATH)
                .option(DSConstant.TABLE_NAME, source.getName())
                .load();
        dataSet.printSchema();
        DefaultETLOperatorJobBuilderContent builderContent = DefaultETLOperatorJobBuilderContent.getInstance();
        for (ETLOperator operator : ETLOperators) {
            ETLOperatorJobBuilder jobBuilder = builderContent.getETLOperatorJobBuilder(operator);
            dataSet = jobBuilder.build(dataSet, operator, content);
        }
        //dataSet.show();
        return new SparkDataSource(dataSet);
    }
}
