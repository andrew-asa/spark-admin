package com.asa.lab.internalimp.operator;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.operator.filter.FilterETLOperator;
import com.asa.lab.internalimp.operator.filter.FilterJobBuilder;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/14.
 */
public class DefaultETLOperatorJobBuilderContent {

    private static DefaultETLOperatorJobBuilderContent INSTANCE;

    private Map<String, ETLOperatorJobBuilder> jobBuilderMap;

    private DefaultETLOperatorJobBuilderContent() {

        init();
    }

    private void init() {

        jobBuilderMap = new HashMap<String, ETLOperatorJobBuilder>();
        setDefaultOperatorJobBuilder();
    }

    private void setDefaultOperatorJobBuilder() {

        jobBuilderMap.put(FilterETLOperator.NAME, new FilterJobBuilder());
    }

    public static DefaultETLOperatorJobBuilderContent getInstance() {

        if (INSTANCE == null) {
            synchronized (DataSourceDriverContent.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DefaultETLOperatorJobBuilderContent();
                }
            }
        }
        return INSTANCE;
    }

    public ETLOperatorJobBuilder getETLOperatorJobBuilder(ETLOperator operator) {

        return jobBuilderMap.get(operator.getName());
    }
}
