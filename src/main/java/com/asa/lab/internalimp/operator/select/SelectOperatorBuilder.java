package com.asa.lab.internalimp.operator.select;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/11/5.
 */
public class SelectOperatorBuilder {

    private List<SelectItem> items;

    public SelectOperatorBuilder() {

        items = new ArrayList<>();
    }

    public SelectOperatorBuilder addItem(String tableName, String columnName, String name) {

        SelectItem item = new SelectItem(tableName, columnName, name);
        items.add(item);
        return this;
    }

    public SelectOperator build() {

        SelectOperator operator = new SelectOperator(items);
        items = null;
        return operator;
    }
}
