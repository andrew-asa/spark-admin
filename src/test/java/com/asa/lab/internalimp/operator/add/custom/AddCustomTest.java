package com.asa.lab.internalimp.operator.add.custom;


import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.etl.DefaultETLBuilder;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperatorHelper;
import com.asa.lab.internalimp.operator.select.SelectOperatorHelper;
import com.asa.lab.structure.base.group.custom.CustomGroupItem;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import com.asa.utils.ListUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 */
public class AddCustomTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }


    @Test
    public void testAddCustomColumn() {

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
                new Type[]{Type.String, Type.String, Type.Double, Type.String},
                new String[]{"column1", "column2", "sum", "newColumn"},
                new Object[][]{
                        {"group1", "group1_1", 1.0, "group1"},
                        {"group1", "group1_2", 2.0, "group1"},
                        {"group1", "group1_3", 3.0, "group1"},
                        {"group2", "group2_1", 4.0, "group2"},
                        {"group2", "group2_2", 5.0, "group2"},
                        {"group2", "group2_3", 6.0, "group2"},
                        {"group3", "group3_1", 7.0, "group3"},
                        {"group3", "group3_2", 8.0, "group3"},
                        {"group3", "group3_3", 9.0, "group3"},
                });
        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        CustomGroupItem item1 = new CustomGroupItem("group1", ListUtils.arrayToList("group1_1", "group1_2", "group1_3"));
        CustomGroupItem item2 = new CustomGroupItem("group2", ListUtils.arrayToList("group2_1", "group2_2", "group2_3"));
        CustomGroupItem item3 = new CustomGroupItem("group3", ListUtils.arrayToList("group3_1", "group3_2", "group3_3"));
        ETLOperators
                .add(AddNewColumnOperatorHelper
                             .buildAddCustomColumn("newColumn", "column2",
                                                   ListUtils.arrayToList(item1, item2, item3),
                                                   false,
                                                   "other"));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void testAddCustomColumn2() {

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
                new Type[]{Type.String, Type.String, Type.Double, Type.String},
                new String[]{"column1", "column2", "sum", "newColumn"},
                new Object[][]{
                        {"group1", "group1_1", 1.0, "group1"},
                        {"group1", "group1_2", 2.0, "other"},
                        {"group1", "group1_3", 3.0, "group1"},
                        {"group2", "group2_1", 4.0, "group2"},
                        {"group2", "group2_2", 5.0, "other"},
                        {"group2", "group2_3", 6.0, "group2"},
                        {"group3", "group3_1", 7.0, "group3"},
                        {"group3", "group3_2", 8.0, "other"},
                        {"group3", "group3_3", 9.0, "group3"},
                });
        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        CustomGroupItem item1 = new CustomGroupItem("group1", ListUtils.arrayToList("group1_1", "group1_3"));
        CustomGroupItem item2 = new CustomGroupItem("group2", ListUtils.arrayToList("group2_1", "group2_3"));
        CustomGroupItem item3 = new CustomGroupItem("group3", ListUtils.arrayToList("group3_1", "group3_3"));
        ETLOperators
                .add(AddNewColumnOperatorHelper
                             .buildAddCustomColumn("newColumn", "column2",
                                                   ListUtils.arrayToList(item1, item2, item3),
                                                   true,
                                                   "other"));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

}