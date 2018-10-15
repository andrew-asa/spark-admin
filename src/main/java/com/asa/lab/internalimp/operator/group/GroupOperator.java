package com.asa.lab.internalimp.operator.group;

import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class GroupOperator implements ETLOperator {

    public static final String NAME = "group";

    @Override
    public String getName() {

        return NAME;
    }
}
