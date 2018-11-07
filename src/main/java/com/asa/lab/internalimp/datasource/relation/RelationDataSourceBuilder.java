package com.asa.lab.internalimp.datasource.relation;

import com.asa.lab.expection.BaseRunTimeException;
import com.asa.lab.internalimp.datasource.BaseColumn;
import com.asa.lab.internalimp.datasource.BaseDataSchema;
import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSchemaHelper;
import com.asa.lab.internalimp.datasource.relation.column.DefaultRelationIndexMapping;
import com.asa.lab.internalimp.datasource.relation.column.ForeignColumnDataSet;
import com.asa.lab.internalimp.datasource.relation.column.PrimaryColumnDataSet;
import com.asa.lab.internalimp.datasource.relation.dataset.RelationDataSet;
import com.asa.lab.internalimp.operator.select.SelectItem;
import com.asa.lab.internalimp.operator.select.SelectOperator;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.ColumnDataSet;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.datasource.relation.Relation;
import com.asa.lab.structure.datasource.relation.RelationIndexMapping;
import com.asa.utils.ComparatorUtils;
import com.asa.utils.ListUtils;
import com.asa.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 */
public class RelationDataSourceBuilder {

    private List<SelectItem> items;

    private DataSchema schema;

    private String primaryTable;

    private DataSource primaryDataSource;

    private DataSchemaHelper primarySchemaHelper;

    private Map<String, RelationColumnInfo> relationColumnInfoMap;

    private RelationDataSet dataSet;

    public DataSource build(SelectOperator selectOperator) {

        buildInternal(selectOperator);
        RelationTablesDataSource relationTablesDataSource = new RelationTablesDataSource(dataSet, schema);
        return relationTablesDataSource;
    }

    private void buildInternal(SelectOperator selectOperator) {

        items = selectOperator.getItems();
        relationColumnInfoMap = new HashMap<>();
        buildPrimaryTableInfo();
        buildRelationInfoMap();
        buildDataSchema();
        buildDataSet();
    }

    private void buildPrimaryTableInfo() {

        items.forEach(item -> {
            String tableName = item.getTableName();
            if (StringUtils.isEmpty(primaryTable)) {
                primaryTable = tableName;
            } else if (!ComparatorUtils.equals(primaryTable, tableName)) {
                Relation relation = DataBaseContent.getInstance().getRelation(primaryTable, tableName);
                if (relation == null) {
                    relation = DataBaseContent.getInstance().getRelation(tableName, primaryTable);
                    if (relation == null) {
                        throw new BaseRunTimeException(
                                StringUtils.messageFormat("select tables {},{} no have relation", primaryTable, tableName)
                        );
                    } else {
                        primaryTable = tableName;
                    }
                }
            }
        });
        primaryDataSource = DataBaseContent.getInstance().getDataSource(primaryTable);
        DataSchema primaryDataSchema = primaryDataSource.getSchema();
        primarySchemaHelper = new DataSchemaHelper(primaryDataSchema);
    }

    private void buildRelationInfoMap() {

        items.forEach(item -> {
            String tableName = item.getTableName();
            String columnName = item.getColumnName();
            if (!ComparatorUtils.equals(tableName, primaryTable)) {
                RelationColumnInfo columnInfo = relationColumnInfoMap.get(tableName);
                if (columnInfo == null) {
                    Relation relation = DataBaseContent.getInstance().getRelation(primaryTable, tableName);
                    columnInfo = new RelationColumnInfo();
                    columnInfo.setRelation(relation);
                    relationColumnInfoMap.put(tableName, columnInfo);
                    DataSource dataSource = DataBaseContent.getInstance().getDataSource(tableName);
                    columnInfo.setForeignDataSource(dataSource);
                    DataSchemaHelper schemaHelper = new DataSchemaHelper(dataSource.getSchema());
                    columnInfo.setSchemaHelper(schemaHelper);
                    String primaryField = relation.getPrimaryFields().get(0);
                    int primaryIndex = primarySchemaHelper.getColumnIndex(primaryField);
                    String foreignField = relation.getForeignFields().get(0);
                    int foreignIndex = schemaHelper.getColumnIndex(foreignField);
                    DefaultRelationIndexMapping defaultRelationIndexMapping = new DefaultRelationIndexMapping(primaryDataSource.getDataSet(), primaryIndex, dataSource.getDataSet(), foreignIndex);
                    columnInfo.setRelationIndexMapping(defaultRelationIndexMapping);
                }
                columnInfo.addColumn(columnName);
            }
        });
    }

    private void buildDataSchema() {

        List<Column> columns = new ArrayList<>();
        if (ListUtils.isNotEmpty(items)) {
            items.forEach(item -> {
                String tableName = item.getTableName();
                String columnName = item.getColumnName();
                String displayName = item.getName();
                DataSchemaHelper schemaHelper;
                if (ComparatorUtils.equals(primaryTable, tableName)) {
                    schemaHelper = primarySchemaHelper;
                } else {
                    schemaHelper = relationColumnInfoMap.get(tableName).getSchemaHelper();
                }
                Type type = schemaHelper.getColumnType(columnName);
                BaseColumn baseColumn = new BaseColumn(type, displayName);
                baseColumn.setTableName(tableName);
                columns.add(baseColumn);
            });
        }
        schema = new BaseDataSchema(columns.toArray(new Column[0]));
    }

    private void buildDataSet() {

        List<ColumnDataSet> columnDataSets = new ArrayList<>();

        if (ListUtils.isNotEmpty(items)) {
            items.forEach(item -> {
                String tableName = item.getTableName();
                String columnName = item.getColumnName();
                DataSchemaHelper schemaHelper;
                int colIndex;
                if (ComparatorUtils.equals(primaryTable, tableName)) {
                    schemaHelper = primarySchemaHelper;
                    colIndex = schemaHelper.getColumnIndex(columnName);
                    PrimaryColumnDataSet primaryColumnDataSet = new PrimaryColumnDataSet(primaryDataSource.getDataSet(), colIndex);
                    columnDataSets.add(primaryColumnDataSet);
                } else {
                    RelationColumnInfo columnInfo = relationColumnInfoMap.get(tableName);
                    schemaHelper = columnInfo.getSchemaHelper();
                    DataSet dataSet = columnInfo.getForeignDataSource().getDataSet();
                    colIndex = schemaHelper.getColumnIndex(columnName);
                    RelationIndexMapping indexMapping = columnInfo.getRelationIndexMapping();
                    ForeignColumnDataSet foreignColumnDataSet = new ForeignColumnDataSet(dataSet, colIndex, indexMapping);
                    columnDataSets.add(foreignColumnDataSet);
                }
            });
        }
        int primaryRowSize = primaryDataSource.getDataSet().getRowSize();
        dataSet = new RelationDataSet(columnDataSets.toArray(new ColumnDataSet[0]), primaryRowSize);
    }
}
