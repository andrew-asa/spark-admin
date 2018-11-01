package com.asa.lab.internalimp.operator.group;

import com.asa.lab.internalimp.operator.add.custom.AddCustomColumn;
import com.asa.lab.internalimp.operator.add.custom.AddCustomDriver;
import com.asa.lab.structure.base.group.GroupColumn;
import com.asa.lab.structure.base.group.GroupType;
import com.asa.lab.structure.base.group.SummaryColumn;
import com.asa.lab.structure.base.group.custom.CustomGroupColumn;
import com.asa.lab.structure.base.summary.SummaryType;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.utils.ComparatorUtils;
import com.asa.utils.ListUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class GroupJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        GroupOperator groupOperator = (GroupOperator) operator;
        dataSet = expanderGroup(groupOperator, dataSet, content);
        dataSet = beforeExpanderSummary(groupOperator, dataSet, content);
        dataSet = expanderSummary(groupOperator, dataSet, content);
        dataSet = afterExpanderSummary(groupOperator, dataSet, content);
        return dataSet;
    }

    private Dataset<Row> expanderGroup(GroupOperator groupOperator, Dataset<Row> dataSet, ETLJobBuilderContent content) {

        List<GroupColumn> groupColumns = groupOperator.getGroups();
        if (ListUtils.isNotEmpty(groupColumns)) {
            for (GroupColumn column : groupColumns) {
                GroupType groupType = column.getGroupType();
                String source = column.getColumnSourceName();
                String display = column.getDisplayName();
                // id分组
                if (GroupType.ID.equals(groupType) && !ComparatorUtils.equals(source, display)) {
                    dataSet = dataSet.withColumn(display, dataSet.col(source));
                } else if (GroupType.CUSTOM.equals(groupType)) {
                    //  自定义分组
                    CustomGroupColumn customGroupColumn = (CustomGroupColumn) column;
                    AddCustomColumn newColumnOperator = new AddCustomColumn(customGroupColumn);
                    newColumnOperator.setColumnName(source);
                    newColumnOperator.setType(Type.String);
                    AddCustomDriver driver = new AddCustomDriver();
                    dataSet = driver.build(dataSet, newColumnOperator, content);
                }
            }
        }
        return dataSet;
    }

    private Dataset<Row> beforeExpanderSummary(GroupOperator groupOperator, Dataset<Row> dataSet, ETLJobBuilderContent content) {

        List<SummaryColumn> summaryColumns = groupOperator.getSummarys();
        if (ListUtils.isNotEmpty(summaryColumns)) {
            for (SummaryColumn column : summaryColumns) {
                String source = column.getColumnSourceName();
                String display = column.getDisplayName();
                if (!ComparatorUtils.equals(source, display)) {
                    dataSet = dataSet.withColumn(display, dataSet.apply(source));
                }
            }
        }
        return dataSet;
    }

    private Dataset<Row> expanderSummary(GroupOperator groupOperator, Dataset<Row> dataSet, ETLJobBuilderContent content) {

        List<GroupColumn> groupColumns = groupOperator.getGroups();
        RelationalGroupedDataset groupedDataset = null;
        if (ListUtils.isNotEmpty(groupColumns)) {
            List<Column> grous = new ArrayList<>();
            for (GroupColumn item : groupColumns) {
                grous.add(dataSet.apply(item.getDisplayName()));
            }
            groupedDataset = dataSet.groupBy(grous.toArray(new Column[0]));
        }
        List<SummaryColumn> summaryColumns = groupOperator.getSummarys();
        List<Column> sColumns = new ArrayList<>();
        if (ListUtils.isNotEmpty(summaryColumns)) {
            summaryColumns.forEach(item -> {
                SummaryType summaryType = item.getSummaryType();
                String displayName = item.getDisplayName();
                String source = item.getColumnSourceName();
                Column column;
                if (SummaryType.max.equals(summaryType)) {
                    column = functions.max(functions.col(source));
                } else if (SummaryType.min.equals(summaryType)) {
                    column = functions.min(functions.col(source));
                } else if (SummaryType.avg.equals(summaryType)) {
                    column = functions.avg(functions.col(source));
                } else {
                    column = functions.sum(functions.col(source));
                }
                column = column.as(displayName);
                sColumns.add(column);
            });
            dataSet = groupedDataset.agg(sColumns.get(0), sColumns.subList(1, sColumns.size()).toArray(new Column[0]));
        }
        return dataSet;
    }

    private Dataset<Row> afterExpanderSummary(GroupOperator groupOperator, Dataset<Row> dataSet, ETLJobBuilderContent content) {

        return dataSet;
    }
}
