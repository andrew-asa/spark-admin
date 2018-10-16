package com.asa.lab.internalimp.operator.add.time;

import com.asa.lab.internalimp.operator.add.AddNewColumnDriver;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.internalimp.operator.add.expression.AddExpressionColumn;
import com.asa.lab.internalimp.sql.relation.ColumnTypeToDataTypeVisitor;
import com.asa.lab.structure.base.time.TimeInterval;
import com.asa.lab.structure.base.time.TimeUtils;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkFunctionsHelper;
import com.asa.utils.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Second;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.time.Clock;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddTimeDiffDriver implements AddNewColumnDriver {

    @Override
    public Dataset<Row> build(Dataset<Row> dataFrame, AddNewColumnOperator operator, ETLJobBuilderContent content) {

        AddTimeDiffColumn addTimeDiffColumn = (AddTimeDiffColumn) operator;
        String columnName = addTimeDiffColumn.getColumnName();
        String starColumnName = addTimeDiffColumn.getStartColumn();
        String endColumnName = addTimeDiffColumn.getEndColumn();
        Type type = addTimeDiffColumn.getType();
        DataType dataType = type.accept(ColumnTypeToDataTypeVisitor.INSTANT);
        TimeInterval timeInterval = addTimeDiffColumn.getInterval();
        long intervalSeconds = TimeUtils.intervalSeconds(addTimeDiffColumn.getInterval());
        // 开始时间是系统时间，结束时间不是系统时间
        Column startColumn;
        Column endColumn;
        Column resultColumn = null;
        if (addTimeDiffColumn.isStartTimeIsSystemTime()) {
            startColumn = functions.current_date();
        } else {
            startColumn = dataFrame.apply(starColumnName);
        }
        if (addTimeDiffColumn.isEndTimeIsSystemTime()) {
            endColumn = functions.current_date();
        } else {
            endColumn = dataFrame.apply(endColumnName);
        }
        resultColumn = endColumn.cast(DataTypes.TimestampType).cast(DataTypes.LongType)
                .minus(startColumn.cast(DataTypes.TimestampType).cast(DataTypes.LongType))
                .divide(intervalSeconds);
        dataFrame.printSchema();
        dataFrame = dataFrame.withColumn(columnName, resultColumn.cast(dataType));
        return dataFrame;
    }


}
