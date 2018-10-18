package com.asa.lab.internalimp.operator.add.custom;

import com.asa.lab.internalimp.operator.add.AddNewColumnDriver;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddCustomDriver implements AddNewColumnDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataFrame, AddNewColumnOperator operator, ETLJobBuilderContent content) {

        AddCustomColumn customColumn = (AddCustomColumn) operator;
        String columnName = customColumn.getColumnName();
        List<StructField> fields = Arrays.asList(dataFrame.schema().fields());
        List<StructField> structFields = new ArrayList<>(fields);
        StructField field = new StructField(columnName, DataTypes.StringType, true, Metadata.empty());
        structFields.add(field);
        StructType structType = DataTypes.createStructType(structFields);
        CustomColumnRowFunction columnRowFunction = new CustomColumnRowFunction(customColumn, structType);
        dataFrame = dataFrame.mapPartitions(columnRowFunction, RowEncoder.apply(structType));
        return dataFrame;
    }
}
