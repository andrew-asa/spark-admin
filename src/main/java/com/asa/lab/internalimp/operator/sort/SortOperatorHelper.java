package com.asa.lab.internalimp.operator.sort;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class SortOperatorHelper {

    public static ColumnSortOperator buildSortOperator(String[] columnsName, boolean[] desc) {

        List<SortItem> columns = new ArrayList<>();
        for (int i = 0; i < columnsName.length; i++) {
            SortItem item = new SortItem(columnsName[i], desc[i]);
            columns.add(item);
        }
        return new ColumnSortOperator(columns);
    }
}
