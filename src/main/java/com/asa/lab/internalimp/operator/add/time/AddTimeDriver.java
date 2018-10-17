package com.asa.lab.internalimp.operator.add.time;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.operator.add.AddNewColumnDriver;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkFunctionsHelper;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 */
public class AddTimeDriver implements AddNewColumnDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataFrame, AddNewColumnOperator operator, ETLJobBuilderContent content) {

        AddTimeColumn addTimeColumn = (AddTimeColumn) operator;
        String colName = addTimeColumn.getColumnName();
        String orgColName = addTimeColumn.getOrgColumnName();
        Type type = addTimeColumn.getType();
        DataType dataType = DataSourceDriverContent.convertToDataType(type);
        Column column = addTimeColumn.isSystemColumn() ?
                functions.current_timestamp()
                : dataFrame.col(orgColName);
        column = SparkFunctionsHelper.timeColumn(column, addTimeColumn.getGroup()).cast(dataType);
        dataFrame = dataFrame.withColumn(colName, column);
        return dataFrame;
    }
}
