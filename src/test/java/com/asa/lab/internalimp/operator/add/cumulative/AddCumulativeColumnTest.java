package com.asa.lab.internalimp.operator.add.cumulative;


import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.etl.DefaultETLBuilder;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperatorHelper;
import com.asa.lab.internalimp.operator.select.SelectOperatorHelper;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import com.asa.utils.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 */
public class AddCumulativeColumnTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }


    @Test
    public void testAddCumulativeColumn_SUM_GROUP() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double},
                new String[]{"column1", "column2", "sum"},
                new Object[][]{
                        {"group1", "group1_1", 1.0},
                        {"group1", "group1_2", 2.0},
                        {"group1", "group1_3", 3.0},
                        {"group2", "group2_1", 4.0},
                        {"group2", "group2_2", 5.0},
                        {"group2", "group2_3", 6.0},
                        {"group3", "group3_1", 7.0},
                        {"group3", "group3_2", 8.0},
                        {"group3", "group3_3", 9.0},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double, Type.Double},
                new String[]{"column1", "column2", "sum", "newColumn"},
                new Object[][]{
                        {"group1", "group1_1", 1.0, 1.0},
                        {"group1", "group1_2", 2.0, 1.0 + 2.0},
                        {"group1", "group1_3", 3.0, 1.0 + 2.0 + 3.0},
                        {"group2", "group2_1", 4.0, 1.0 + 2.0 + 3.0 + 4.0},
                        {"group2", "group2_2", 5.0, 1.0 + 2.0 + 3.0 + 4.0 + 5.0},
                        {"group2", "group2_3", 6.0, 1.0 + 2.0 + 3.0 + 4.0 + 5.0 + 6.0},
                        {"group3", "group3_1", 7.0, 1.0 + 2.0 + 3.0 + 4.0 + 5.0 + 6.0 + 7.0},
                        {"group3", "group3_2", 8.0, 1.0 + 2.0 + 3.0 + 4.0 + 5.0 + 6.0 + 7.0 + 8.0},
                        {"group3", "group3_3", 9.0, 1.0 + 2.0 + 3.0 + 4.0 + 5.0 + 6.0 + 7.0 + 8.0 + 9.0},
                });
        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddCumulativeColumn("newColumn", "sum", ArrayUtils.arrayToList(new String[]{"column1"}), false));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddCumulativeColumn_SUM_IN_GROUP() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double},
                new String[]{"column1", "column2", "sum"},
                new Object[][]{
                        {"group1", "group1_1", 1.0},
                        {"group1", "group1_2", 2.0},
                        {"group1", "group1_3", 3.0},
                        {"group2", "group2_1", 4.0},
                        {"group2", "group2_2", 5.0},
                        {"group2", "group2_3", 6.0},
                        {"group3", "group3_1", 7.0},
                        {"group3", "group3_2", 8.0},
                        {"group3", "group3_3", 9.0},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double, Type.Double},
                new String[]{"column1", "column2", "sum", "newColumn"},
                new Object[][]{
                        {"group1", "group1_1", 1.0, 1.0},
                        {"group1", "group1_2", 2.0, 1.0 + 2.0},
                        {"group1", "group1_3", 3.0, 1.0 + 2.0 + 3.0},
                        {"group2", "group2_1", 4.0, 4.0},
                        {"group2", "group2_2", 5.0, 4.0 + 5.0},
                        {"group2", "group2_3", 6.0, 4.0 + 5.0 + 6.0},
                        {"group3", "group3_1", 7.0, 7.0},
                        {"group3", "group3_2", 8.0, 7.0 + 8.0},
                        {"group3", "group3_3", 9.0, 7.0 + 8.0 + 9.0},
                });
        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddCumulativeColumn("newColumn", "sum", ArrayUtils.arrayToList(new String[]{"column1"}), true));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddCumulativeColumn_SUM_IN_GROUP2() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double},
                new String[]{"column1", "column2", "sum"},
                new Object[][]{
                        {"group1", "group1_1", 1.0},
                        {"group1", "group1_3", 3.0},
                        {"group2", "group2_1", 4.0},
                        {"group2", "group2_2", 5.0},
                        {"group1", "group1_2", 2.0},
                        {"group2", "group2_3", 6.0},
                        {"group3", "group3_1", 7.0},
                        {"group3", "group3_2", 8.0},
                        {"group3", "group3_3", 9.0},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double, Type.Double},
                new String[]{"column1", "column2", "sum", "newColumn"},
                new Object[][]{
                        {"group1", "group1_1", 1.0, 1.0},
                        {"group1", "group1_3", 3.0, 1.0 + 3.0},
                        {"group2", "group2_1", 4.0, 4.0},
                        {"group2", "group2_2", 5.0, 4.0 + 5.0},
                        {"group1", "group1_2", 2.0, 1.0 + 3.0 + 2.0},
                        {"group2", "group2_3", 6.0, 4.0 + 5.0 + 6.0},
                        {"group3", "group3_1", 7.0, 7.0},
                        {"group3", "group3_2", 8.0, 7.0 + 8.0},
                        {"group3", "group3_3", 9.0, 7.0 + 8.0 + 9.0},
                });
        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddCumulativeColumn("newColumn", "sum", ArrayUtils.arrayToList(new String[]{"column1"}), true));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }
}