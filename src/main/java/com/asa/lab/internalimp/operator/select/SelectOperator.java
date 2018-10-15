package com.asa.lab.internalimp.operator.select;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 选字段
 */
public class SelectOperator implements ETLOperator {

    public static final String NAME = "select";

    /**
     * 选择的列
     */
    private List<SelectItem> items;

    public SelectOperator(List<SelectItem> items) {

        this.items = items;
    }

    public static String getNAME() {

        return NAME;
    }

    public List<SelectItem> getItems() {

        return items;
    }

    public void setItems(List<SelectItem> items) {

        this.items = items;
    }

    @Override
    public String getName() {

        return NAME;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof SelectOperator)) {
            return false;
        }

        SelectOperator that = (SelectOperator) o;
        return AssistUtils.equals(items, that.items);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(items);
    }
}
