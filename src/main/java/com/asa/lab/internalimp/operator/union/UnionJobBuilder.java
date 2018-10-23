package com.asa.lab.internalimp.operator.union;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import com.asa.utils.ListUtils;
import com.asa.utils.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class UnionJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        UnionOperator unionOperator = (UnionOperator) operator;
        List<UnionResultColumnItem> resultColumnItems = unionOperator.getResult();
        UnionTableItem currentTable = unionOperator.getCurrentTable();
        if (ListUtils.isNotEmpty(resultColumnItems)) {
            dataSet = rebuildCurrentTable(dataSet, currentTable, resultColumnItems);
            List<UnionTableItem> unionTableItems = unionOperator.getUnionTables();
            if (ListUtils.isNotEmpty(unionTableItems)) {
                for (UnionTableItem unionTableItem : unionTableItems) {
                    Dataset<Row> unionTable = buildUnionTable(unionTableItem, resultColumnItems);
                    dataSet = dataSet.union(unionTable);
                }
            }
            return dataSet;
        }
        return dataSet;
    }

    private Dataset<Row> rebuildCurrentTable(Dataset<Row> dataSet, UnionTableItem tableItem, List<UnionResultColumnItem> resultColumnItems) {

        List<UnionColumnItem> columnItems = tableItem.getColumnItems();
        List<Column> columns = new ArrayList<>();
        int len = ListUtils.length(columnItems);
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                UnionColumnItem unionColumnItem = columnItems.get(i);
                UnionResultColumnItem resultColumnItem = resultColumnItems.get(i);
                String columnName = unionColumnItem.getColumnName();
                Column column;
                String name = resultColumnItem.getName();
                if (StringUtils.isNotEmpty(columnName)) {
                    column = dataSet.col(columnName);
                } else {
                    column = functions.lit(null);
                }
                column.as(name);
                Type type = resultColumnItem.getType();
                column.cast(DataSourceDriverContent.convertToDataType(type));
                columns.add(column);
            }
        }
        return dataSet.select(columns.toArray(new Column[0]));
    }

    private Dataset<Row> buildUnionTable(UnionTableItem tableItem, List<UnionResultColumnItem> resultColumnItems) {

        Dataset<Row> dataFrame = SparkContentManager.getInstance().getDataset(tableItem.getTableName());
        return rebuildCurrentTable(dataFrame, tableItem, resultColumnItems);
    }
}
