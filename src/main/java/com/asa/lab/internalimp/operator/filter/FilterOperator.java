package com.asa.lab.internalimp.operator.filter;

import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public abstract class FilterOperator implements ETLOperator, FilterConditionName {

    public static final String NAME = "filter";

    @Override
    public String getName() {

        return NAME;
    }
}
