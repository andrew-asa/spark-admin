package com.asa.lab.internalimp.operator.union;

import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class UnionOperator implements ETLOperator {

    public static final String NAME = "union";

    @Override
    public String getName() {

        return NAME;
    }
}
