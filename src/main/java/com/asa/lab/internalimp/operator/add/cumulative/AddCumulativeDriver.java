package com.asa.lab.internalimp.operator.add.cumulative;

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
public class AddCumulativeDriver implements AddNewColumnDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataFrame, AddNewColumnOperator operator, ETLJobBuilderContent content) {

        AddCumulativeColumn cumulativeColumn = (AddCumulativeColumn) operator;
        String colName = cumulativeColumn.getColumnName();
        boolean inGroup = cumulativeColumn.isInGroup();
        String sumCol = cumulativeColumn.getSummaryColumn();
        List<String> groupColumns = cumulativeColumn.getGroupColumns();
        Column column;
        dataFrame = dataFrame.withColumn(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME, functions.monotonically_increasing_id());
        if (inGroup) {
            column = SparkFunctionsHelper.cumulativeInGroup(sumCol, groupColumns);
        } else {
            column = SparkFunctionsHelper.cumulativeInAll(sumCol);
        }
        dataFrame = dataFrame.withColumn(colName, column).orderBy(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME);
        dataFrame = dataFrame.drop(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME);
        return dataFrame;
    }
}
