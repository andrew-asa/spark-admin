package com.asa.lab.internalimp.operator.filter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class FilterOperatorHelper {

    public static ColumnFilterETLOperator buildColumnFilter(String columnName, Object... values) {

        List<Object> fv = new ArrayList<>();
        for (Object o : values) {
            fv.add(o);
        }
        ColumnFilterETLOperator filterOperator = new ColumnFilterETLOperator(columnName, fv);
        return filterOperator;
    }
}
