package com.asa.lab.internalimp.operator.select;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class SelectOperatorHelper {

    public static SelectOperator buildSelectOperator(String[] tablesName, String[] columnsName) {

        List<SelectItem> columns = new ArrayList<>();
        for (int i = 0; i < columnsName.length; i++) {
            SelectItem item = new SelectItem(tablesName[i], columnsName[i]);
            columns.add(item);
        }
        return new SelectOperator(columns);
    }
}
