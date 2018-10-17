package com.asa.lab.internalimp.operator.add.summary;

import com.asa.lab.expection.DSUnSupportException;
import com.asa.lab.internalimp.operator.add.AddNewColumnDriver;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.base.summary.SummaryType;
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
public class AddSummaryDriver implements AddNewColumnDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataFrame, AddNewColumnOperator operator, ETLJobBuilderContent content) {

        AddSummaryColumn addSummaryColumn = (AddSummaryColumn) operator;
        String summaryColumn = addSummaryColumn.getSummaryColumn();
        SummaryType summaryType = addSummaryColumn.getSummaryType();
        List<String> groupColumns = addSummaryColumn.getGroupColumns();
        boolean isInGroup = addSummaryColumn.isInGroup();
        String colName = addSummaryColumn.getColumnName();
        Column column;
        dataFrame = dataFrame.withColumn(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME, functions.monotonically_increasing_id());
        if (isInGroup) {
            column = summaryInGroup(summaryType, summaryColumn, groupColumns);
        } else {
            column = summaryInAll(summaryType, summaryColumn);
        }
        dataFrame = dataFrame.withColumn(colName, column);
        dataFrame = dataFrame.orderBy(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME).drop(SparkFunctionsHelper.ROW_INDEX_COLUMN_NAME);
        return dataFrame;
    }

    private Column summaryInGroup(SummaryType summaryType, String summaryColumnName, List<String> groupsColumnNames) {

        switch (summaryType) {
            case sum:
                return SparkFunctionsHelper.sumInGroup(summaryColumnName, groupsColumnNames);
            case max:
                return SparkFunctionsHelper.maxInGroup(summaryColumnName, groupsColumnNames);
            case min:
                return SparkFunctionsHelper.minInGroup(summaryColumnName, groupsColumnNames);
            case avg:
                return SparkFunctionsHelper.avgInGroup(summaryColumnName, groupsColumnNames);
            default:
                throw new DSUnSupportException("un support summary type {}", summaryType);
        }
    }

    private Column summaryInAll(SummaryType summaryType, String summaryColumnName) {

        switch (summaryType) {
            case sum:
                return SparkFunctionsHelper.sumInAll(summaryColumnName);
            case max:
                return SparkFunctionsHelper.maxInAll(summaryColumnName);
            case min:
                return SparkFunctionsHelper.minInAll(summaryColumnName);
            case avg:
                return SparkFunctionsHelper.avgInAll(summaryColumnName);
            default:
                throw new DSUnSupportException("un support summary type {}", summaryType);
        }
    }
}
