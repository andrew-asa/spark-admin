package com.asa.lab.internalimp.operator;

import com.asa.lab.internalimp.datasource.driver.DataSourceDriverContent;
import com.asa.lab.internalimp.operator.add.AddNewColumnJobBuilder;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.internalimp.operator.filter.FilterOperator;
import com.asa.lab.internalimp.operator.filter.FilterJobBuilder;
import com.asa.lab.internalimp.operator.group.GroupJobBuilder;
import com.asa.lab.internalimp.operator.group.GroupOperator;
import com.asa.lab.internalimp.operator.join.JoinJobBuilder;
import com.asa.lab.internalimp.operator.join.JoinOperator;
import com.asa.lab.internalimp.operator.select.SelectJobBuilder;
import com.asa.lab.internalimp.operator.select.SelectOperator;
import com.asa.lab.internalimp.operator.setting.FieldSettingJobBuilder;
import com.asa.lab.internalimp.operator.setting.FieldSettingOperator;
import com.asa.lab.internalimp.operator.sort.ColumnSortJobBuilder;
import com.asa.lab.internalimp.operator.sort.ColumnSortOperator;
import com.asa.lab.internalimp.operator.union.UnionJobBuilder;
import com.asa.lab.internalimp.operator.union.UnionOperator;
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
        jobBuilderMap.put(ColumnSortOperator.NAME, new ColumnSortJobBuilder());
        jobBuilderMap.put(AddNewColumnOperator.NAME, new AddNewColumnJobBuilder());
        jobBuilderMap.put(GroupOperator.NAME, new GroupJobBuilder());
        jobBuilderMap.put(FieldSettingOperator.NAME, new FieldSettingJobBuilder());
        jobBuilderMap.put(JoinOperator.NAME, new JoinJobBuilder());
        jobBuilderMap.put(UnionOperator.NAME, new UnionJobBuilder());
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
