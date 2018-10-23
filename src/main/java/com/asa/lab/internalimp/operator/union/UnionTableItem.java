package com.asa.lab.internalimp.operator.union;

import com.asa.utils.AssistUtils;
import com.asa.utils.ListUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 * 依赖的表的字段信息
 */
public class UnionTableItem {

    /**
     * 参与union的列
     */
    private List<UnionColumnItem> columnItems;

    /**
     * 表名--为空表示当前表
     */
    private String tableName;

    public List<UnionColumnItem> getColumnItems() {

        return columnItems;
    }

    public void setColumnItems(List<UnionColumnItem> columnItems) {

        this.columnItems = columnItems;
    }

    public void addColumnItem(UnionColumnItem columnItem) {

        columnItems = ListUtils.ensureNotNull(columnItems);
        columnItems.add(columnItem);
    }

    public String getTableName() {

        return tableName;
    }

    public void setTableName(String tableName) {

        this.tableName = tableName;
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
