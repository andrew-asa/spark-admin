package com.asa.lab.internalimp.operator.filter;

import com.asa.lab.internalimp.datasource.BaseColumn;
import com.asa.lab.internalimp.operator.filter.column.ColumnInFilterOperator;
import com.asa.lab.internalimp.operator.filter.column.ColumnNotInFilterOperator;
import com.asa.lab.internalimp.operator.select.SelectItem;
import com.asa.lab.internalimp.operator.select.SelectOperator;
import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class FilterOperatorHelper {




    public static ColumnInFilterOperator buildColumnInFilter(String columnName, Object... values) {

        List<Object> fv = new ArrayList<>();
        for (Object o : values) {
            fv.add(o);
        }
        ColumnInFilterOperator filterOperator = new ColumnInFilterOperator(columnName, fv);
        return filterOperator;
    }

    public static ColumnInFilterOperator buildColumnNotInFilter(String columnName, Object... values) {

        List<Object> fv = new ArrayList<>();
        for (Object o : values) {
            fv.add(o);
        }
        ColumnNotInFilterOperator filterOperator = new ColumnNotInFilterOperator(columnName, fv);
        return filterOperator;
    }
}
