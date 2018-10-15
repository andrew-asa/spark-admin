package com.asa.lab.internalimp.operator;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.operator.filter.FilterOperator;
import com.asa.lab.internalimp.operator.filter.FilterJobBuilder;
import com.asa.lab.internalimp.operator.select.SelectJobBuilder;
import com.asa.lab.internalimp.operator.select.SelectOperator;
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

        jobBuilderMap.put(FilterOperator.NAME, new FilterJobBuilder());
        jobBuilderMap.put(SelectOperator.NAME, new SelectJobBuilder());
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

        ETLOperatorJobBuilder builder = jobBuilderMap.get(operator.getName());
        if (builder == null) {
            throw new RuntimeException("no suit operator job builder");
        }
        return builder;
    }
}
