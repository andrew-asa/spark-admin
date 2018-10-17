package com.asa.lab.internalimp.operator.add.time;

import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.etl.DefaultETLBuilder;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperatorHelper;
import com.asa.lab.internalimp.operator.select.SelectOperatorHelper;
import com.asa.lab.structure.base.time.TimeInterval;
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
 * @date 2018/10/16.
 */
public class AddTimeDiffTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testAddTimeDiffColumn_YEAR() {

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
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2069-01-01"), 51},
                        {"lisi", Date.valueOf("2018-01-01"), Date.valueOf("2018-08-01"), 0},
                        {"wangwu", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01"), 1},
                        {"wangwu", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01"), 1},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeDiffColumn("newColumn", "born", "dead", TimeInterval.Year, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddTimeDiffColumn_YEAR_CURRENT_TIME() {

        java.util.Date now = new java.util.Date();
        int year = now.getYear() + 1900;
        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date},
                new String[]{"name", "born", "dead"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), null},
                        {"zhangsan", Date.valueOf("2017-01-01"), null,},
                        {"zhangsan", Date.valueOf("2016-01-01"), null,},
                        {"zhangsan", Date.valueOf("2016-02-01"), null,},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date, Type.Date},
                new String[]{"name", "born", "dead", "newColumn"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), null, year - 2018},
                        {"zhangsan", Date.valueOf("2017-01-01"), null, year - 2017},
                        {"zhangsan", Date.valueOf("2016-01-01"), null, year - 2016},
                        {"zhangsan", Date.valueOf("2016-02-01"), null, year - 2016},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeDiffColumn("newColumn", "born", "", TimeInterval.Year, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddTimeDiffColumn_YEAR_CURRENT_TIME2() {

        java.util.Date now = new java.util.Date();
        int year = now.getYear() + 1900;
        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date},
                new String[]{"name", "born", "dead"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2018-01-01")},
                        {"zhangsan", Date.valueOf("2017-01-01"), Date.valueOf("2017-01-01")},
                        {"zhangsan", Date.valueOf("2016-01-01"), Date.valueOf("2016-01-01")},
                        {"zhangsan", Date.valueOf("2016-02-01"), Date.valueOf("2016-02-01")},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date, Type.Date},
                new String[]{"name", "born", "dead", "newColumn"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2018-01-01"), 2018 - year},
                        {"zhangsan", Date.valueOf("2017-01-01"), Date.valueOf("2017-01-01"), 2017 - year},
                        {"zhangsan", Date.valueOf("2016-01-01"), Date.valueOf("2016-01-01"), 2016 - year},
                        {"zhangsan", Date.valueOf("2016-02-01"), Date.valueOf("2016-02-01"), 2016 - year},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeDiffColumn("newColumn", "", "dead", TimeInterval.Year, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddTimeDiffColumn_SEASON() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date},
                new String[]{"name", "born", "dead"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01")},
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2020-01-01")},
                        {"zhangsan", Date.valueOf("2018-04-09"), Date.valueOf("2020-01-01")},
                        {"zhangsan", Date.valueOf("2020-01-01"), Date.valueOf("2018-01-01")},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date, Type.Int},
                new String[]{"name", "born", "dead", "newColumn"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2019-01-01"), 4},
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2020-01-01"), 8},
                        {"zhangsan", Date.valueOf("2018-04-09"), Date.valueOf("2020-01-01"), 7},
                        {"zhangsan", Date.valueOf("2020-01-01"), Date.valueOf("2018-01-01"), -8},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeDiffColumn("newColumn", "born", "dead", TimeInterval.Season, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddTimeDiffColumn_MONTH() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date},
                new String[]{"name", "born", "dead"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2018-02-01")},
                        {"zhangsan", Date.valueOf("2018-02-01"), Date.valueOf("2018-01-13")},
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2018-02-18")},
                        {"zhangsan", Date.valueOf("2019-01-01"), Date.valueOf("2018-01-01")},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Date, Type.Date, Type.Int},
                new String[]{"name", "born", "dead", "newColumn"},
                new Object[][]{
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2018-02-01"), 1},
                        {"zhangsan", Date.valueOf("2018-02-01"), Date.valueOf("2018-01-13"), 0},
                        {"zhangsan", Date.valueOf("2018-01-01"), Date.valueOf("2018-02-18"), 1},
                        {"zhangsan", Date.valueOf("2019-01-01"), Date.valueOf("2018-01-01"), -12},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddTimeDiffColumn("newColumn", "born", "dead", TimeInterval.Month, Type.Int));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }
}