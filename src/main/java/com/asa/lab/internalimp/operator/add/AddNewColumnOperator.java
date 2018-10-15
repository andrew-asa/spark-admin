package com.asa.lab.internalimp.operator.add;

import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 新增列
 */
public class AddNewColumnOperator implements ETLOperator{

    public static final String NAME = "addNewColumn";

    @Override
    public String getName() {

        return NAME;
    }
}
