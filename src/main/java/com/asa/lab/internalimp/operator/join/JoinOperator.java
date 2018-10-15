package com.asa.lab.internalimp.operator.join;

import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class JoinOperator implements ETLOperator {

    public static final String NAME = "join";


    @Override
    public String getName() {

        return NAME;
    }
}
