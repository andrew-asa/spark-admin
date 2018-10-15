package com.asa.lab.internalimp.operator.sort;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 列排序
 */
public class ColumnSortOperator implements ETLOperator {

    public static final String NAME = "sort";

    private List<SortItem> sortList;

    public ColumnSortOperator(List<SortItem> sortList) {

        this.sortList = sortList;
    }

    @Override
    public String getName() {

        return NAME;
    }

    public static String getNAME() {

        return NAME;
    }

    public List<SortItem> getSortList() {

        return sortList;
    }

    public void setSortList(List<SortItem> sortList) {

        this.sortList = sortList;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnSortOperator)) {
            return false;
        }

        ColumnSortOperator that = (ColumnSortOperator) o;
        return AssistUtils.equals(sortList, that.sortList);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(sortList);
    }
}
