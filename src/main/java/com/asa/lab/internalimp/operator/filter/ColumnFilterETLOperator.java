package com.asa.lab.internalimp.operator.filter;

import com.asa.lab.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 * 列过滤
 */
public class ColumnFilterETLOperator extends FilterETLOperator {

    private String columnName;

    private List<Object> value;

    public ColumnFilterETLOperator(String columnName, List<Object> value) {

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
        if (!(o instanceof ColumnFilterETLOperator)) {
            return false;
        }
        ColumnFilterETLOperator that = (ColumnFilterETLOperator) o;
        return AssistUtils.equals(columnName, that.columnName) &&
                AssistUtils.equals(value, that.value);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(columnName, value);
    }
}
