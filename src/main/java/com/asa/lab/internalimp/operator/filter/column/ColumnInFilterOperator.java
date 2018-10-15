package com.asa.lab.internalimp.operator.filter.column;

import com.asa.lab.internalimp.operator.filter.FilterOperator;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 * 列过滤
 * in(xx,xx)
 */
public class ColumnInFilterOperator extends FilterOperator {

    private String columnName;

    private List<Object> value;

    public static final String CONDITION_NAME = "COLUMN_IN";

    public ColumnInFilterOperator(String columnName, List<Object> value) {

        this.columnName = columnName;
        this.value = value;
    }

    public String getColumnName() {

        return columnName;
    }

    public void setColumnName(String columnName) {

        this.columnName = columnName;
    }

    public List<Object> getValue() {

        return value;
    }

    public void setValue(List<Object> value) {

        this.value = value;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnInFilterOperator)) {
            return false;
        }
        ColumnInFilterOperator that = (ColumnInFilterOperator) o;
        return AssistUtils.equals(columnName, that.columnName) &&
                AssistUtils.equals(value, that.value);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(columnName, value);
    }

    @Override
    public String getConditionName() {

        return CONDITION_NAME;
    }
}
