package com.asa.lab.internalimp.operator.select;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSource;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class SelectOperatorHelper {

    public static SelectOperator buildSelectOperator(String[] tablesName, String[] columnsName) {

        return buildSelectOperator(tablesName, columnsName, columnsName);
    }

    public static SelectOperator buildSelectOperator(String[] tablesName, String[] columnsName, String[] names) {

        List<SelectItem> columns = new ArrayList<>();
        for (int i = 0; i < columnsName.length; i++) {
            SelectItem item = new SelectItem(tablesName[i], columnsName[i], names[i]);
            columns.add(item);
        }
        return new SelectOperator(columns);
    }

    public static SelectOperator allSelectOperator(DataSource dataSource) {

        String tableName = dataSource.getName();
        Column[] columns = dataSource.getSchema().getColumns();
        List<SelectItem> selectItems = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            String colName = columns[i].getName();
            SelectItem item = new SelectItem(tableName, colName, colName);
            selectItems.add(item);
        }
        return new SelectOperator(selectItems);
    }
}
