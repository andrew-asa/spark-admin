package com.asa.lab.internalimp.operator.add.rank;

import com.asa.lab.internalimp.operator.add.AddNewColumnDriver;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkFunctionsHelper;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.List;


/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddRankDriver implements AddNewColumnDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataFrame, AddNewColumnOperator operator, ETLJobBuilderContent content) {

        AddRankColumn addRankColumn = (AddRankColumn) operator;
        String colName = addRankColumn.getColumnName();
        boolean inGroup = addRankColumn.isInGroup();
        boolean desc = addRankColumn.isDesc();
        String sumCol = addRankColumn.getSummaryColumn();
        List<String> groupColumns = addRankColumn.getGroupColumns();
        Column column;
        dataFrame = dataFrame.withColumn(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME, functions.monotonically_increasing_id());
        if (inGroup) {
            column = SparkFunctionsHelper.rankInGroup(sumCol, groupColumns, desc);
        } else {
            column = SparkFunctionsHelper.rankInAll(sumCol, desc);
        }
        dataFrame = dataFrame.withColumn(colName, column).orderBy(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME);
        dataFrame = dataFrame.drop(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME);
        return dataFrame;
    }
}
