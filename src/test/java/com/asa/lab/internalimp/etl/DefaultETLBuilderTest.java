package com.asa.lab.internalimp.etl;


import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.operator.filter.ColumnFilterETLOperator;
import com.asa.lab.internalimp.operator.filter.FilterOperatorHelper;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class DefaultETLBuilderTest {

    @After
    public void after() {

    }

    @Test
    public void testColumnFilter() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                        {"广东", "广州"},
                });

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ColumnFilterETLOperator columnFilterOperator = FilterOperatorHelper.buildColumnFilter("sheng", "江苏", "浙江");
        ETLOperators.add(columnFilterOperator);
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(dataSource, content, ETLOperators);
        DataSourceHelper.show(result);
        Assert.assertTrue(DataSourceHelper.equalDataSet(result, expect));
    }
}