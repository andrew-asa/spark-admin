package com.asa.lab.internalimp.operator.setting;

import com.asa.lab.structure.operator.ETLOperator;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class FieldSettingOperator implements ETLOperator {

    public static final String NAME = "fieldSetting";


    @Override
    public String getName() {

        return NAME;
    }
}
