package com.asa.lab.internalimp.operator.union;

import com.asa.lab.structure.datasource.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 */
public class UnionOperatorBuilder {

    private List<UnionResultColumnItem> result;

    /**
     * 当前表合并依据
     */
    private UnionTableItem currentTable;

    /**
     * union 依赖的表
     */
    private List<UnionTableItem> unionTables;

    public UnionOperator build() {

        UnionOperator operator = new UnionOperator();
        operator.setCurrentTable(currentTable);
        operator.setResult(result);
        operator.setUnionTables(unionTables);
        return operator;
    }

    public UnionOperatorBuilder startCurrentTalbe() {

        currentTable = new UnionTableItem();
        return this;
    }

    public UnionOperatorBuilder addCurrentItem(String... columns) {

        for (String column : columns) {
            UnionColumnItem columnItem = new UnionColumnItem();
            columnItem.setColumnName(column);
            currentTable.addColumnItem(columnItem);
        }
        return this;
    }

    public UnionOperatorBuilder endCurrentTalbe() {

        return this;
    }

    public UnionOperatorBuilder startTableResult() {

        result = new ArrayList<>();
        return this;
    }

    public UnionOperatorBuilder addResultItem(String name, Type type) {

        UnionResultColumnItem resultColumnItem = new UnionResultColumnItem(name, type);
        result.add(resultColumnItem);
        return this;
    }

    public UnionOperatorBuilder endTableResult() {

        return this;
    }

    public UnionOperatorBuilder startUnionTables() {

        unionTables = new ArrayList<>();
        return this;
    }

    public UnionOperatorBuilder addUnionTables(String tableName, String... columns) {

        UnionTableItem tableItem = new UnionTableItem();
        tableItem.setTableName(tableName);
        for (String column : columns) {
            UnionColumnItem columnItem = new UnionColumnItem();
            columnItem.setTableName(tableName);
            columnItem.setColumnName(column);
            tableItem.addColumnItem(columnItem);
        }
        unionTables.add(tableItem);
        return this;
    }

    public UnionOperatorBuilder endUnionTables() {

        return this;
    }
}
