package com.asa.lab.internalimp.operator.add.time;


import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.etl.DefaultETLBuilder;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperatorHelper;
import com.asa.lab.internalimp.operator.select.SelectOperatorHelper;
import com.asa.lab.structure.base.time.TimeGroup;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 */
public class AddTimeColumnTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testAddTimeColumn_YEAR() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date},
                new String[]{"name", "born", "dead"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2069-01-01")},
                        {"lisi", Date.valueOf("2018-01-01"), Date.valueOf("2018-08-01")},
                        {"wangwu", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01")},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date, Type.Date},
                new String[]{"name", "born", "dead", "newColumn"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2069-01-01"), 2069},
                        {"lisi", Date.valueOf("2018-01-01"), Date.valueOf("2018-08-01"), 2018},
                        {"wangwu", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01"), 2019},
                        {"wangwu", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01"), 2019},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeColumn("newColumn", "dead", TimeGroup.Year, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddTimeColumn_Season() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date},
                new String[]{"name", "born"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01")},
                        {"lisi", Date.valueOf("2018-02-01")},
                        {"wangwu", Date.valueOf("2018-03-01")},
                        {"wangwu", Date.valueOf("2018-04-01")},
                        {"wangwu", Date.valueOf("2018-05-01")},
                        {"wangwu", Date.valueOf("2018-06-01")},
                        {"wangwu", Date.valueOf("2018-07-01")},
                        {"wangwu", Date.valueOf("2018-08-01")},
                        {"wangwu", Date.valueOf("2018-09-01")},
                        {"wangwu", Date.valueOf("2018-10-01")},
                        {"wangwu", Date.valueOf("2018-11-01")},
                        {"wangwu", Date.valueOf("2018-12-01")},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Int},
                new String[]{"name", "born", "newColumn"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), 1},
                        {"lisi", Date.valueOf("2018-02-01"), 1},
                        {"wangwu", Date.valueOf("2018-03-01"), 1},
                        {"wangwu", Date.valueOf("2018-04-01"), 2},
                        {"wangwu", Date.valueOf("2018-05-01"), 2},
                        {"wangwu", Date.valueOf("2018-06-01"), 2},
                        {"wangwu", Date.valueOf("2018-07-01"), 3},
                        {"wangwu", Date.valueOf("2018-08-01"), 3},
                        {"wangwu", Date.valueOf("2018-09-01"), 3},
                        {"wangwu", Date.valueOf("2018-10-01"), 4},
                        {"wangwu", Date.valueOf("2018-11-01"), 4},
                        {"wangwu", Date.valueOf("2018-12-01"), 4},
                });
        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeColumn("newColumn", "born", TimeGroup.Season, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddTimeColumn_Month() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date},
                new String[]{"name", "born", "dead"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2069-01-01")},
                        {"lisi", Date.valueOf("2018-02-01"), Date.valueOf("2018-08-01")},
                        {"wangwu", Date.valueOf("2018-03-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-04-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-05-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-06-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-07-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-08-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-09-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-10-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-11-01"), Date.valueOf("2019-01-01")},
                        {"wangwu", Date.valueOf("2018-12-01"), Date.valueOf("2019-01-01")},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date, Type.Date},
                new String[]{"name", "born", "dead", "newColumn"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2069-01-01"), 01},
                        {"lisi", Date.valueOf("2018-02-01"), Date.valueOf("2018-08-01"), 02},
                        {"wangwu", Date.valueOf("2018-03-01"), Date.valueOf("2019-01-01"), 03},
                        {"wangwu", Date.valueOf("2018-04-01"), Date.valueOf("2019-01-01"), 04},
                        {"wangwu", Date.valueOf("2018-05-01"), Date.valueOf("2019-01-01"), 05},
                        {"wangwu", Date.valueOf("2018-06-01"), Date.valueOf("2019-01-01"), 06},
                        {"wangwu", Date.valueOf("2018-07-01"), Date.valueOf("2019-01-01"), 07},
                        {"wangwu", Date.valueOf("2018-08-01"), Date.valueOf("2019-01-01"), 8},
                        {"wangwu", Date.valueOf("2018-09-01"), Date.valueOf("2019-01-01"), 9},
                        {"wangwu", Date.valueOf("2018-10-01"), Date.valueOf("2019-01-01"), 10},
                        {"wangwu", Date.valueOf("2018-11-01"), Date.valueOf("2019-01-01"), 11},
                        {"wangwu", Date.valueOf("2018-12-01"), Date.valueOf("2019-01-01"), 12},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeColumn("newColumn", "born", TimeGroup.Month, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }
}