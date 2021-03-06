package com.asa.lab.internalimp.etl;


import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperatorHelper;
import com.asa.lab.internalimp.operator.filter.column.ColumnInFilterOperator;
import com.asa.lab.internalimp.operator.filter.FilterOperatorHelper;
import com.asa.lab.internalimp.operator.select.SelectOperator;
import com.asa.lab.internalimp.operator.select.SelectOperatorHelper;
import com.asa.lab.internalimp.operator.sort.ColumnSortOperator;
import com.asa.lab.internalimp.operator.sort.SortOperatorHelper;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.Type;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 */
public class DefaultETLBuilderTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testAddExpressionColumn() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double},
                new String[]{"sheng", "shi", "gdp"},
                new Object[][]{
                        {"江苏", "南京", 17d},
                        {"浙江", "宁波", 25d},
                        {"江苏", "南通", 34d},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double, Type.Double},
                new String[]{"sheng", "shi", "gdp", "newColumn"},
                new Object[][]{
                        {"江苏", "南京", 17d, 17d * 2 + 3},
                        {"浙江", "宁波", 25d, 25d * 2 + 3},
                        {"江苏", "南通", 34d, 34d * 2 + 3},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(AddNewColumnOperatorHelper.buildAddExpressionColumn("newColumn", "gdp*2+3"));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        DataSourceHelper.show(result);
        Assert.assertTrue(DataSourceHelper.equalDataSet(result, expect));
    }

    @Test
    public void testSort() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                        {"江苏", "南通"},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"浙江", "宁波"},
                        {"江苏", "南京"},
                        {"江苏", "南通"},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ColumnSortOperator columnFilterOperator = SortOperatorHelper.buildSortOperator(new String[]{"sheng"}, new boolean[]{true});
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(columnFilterOperator);
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        DataSourceHelper.show(result);
        Assert.assertTrue(DataSourceHelper.equalDataSet(result, expect));
    }


    @Test
    public void testSelect() {

        String tableName = DataSourceHelper.createDataSourceName();
        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                tableName,
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                        {"广东", "广州"},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                        {"广东", "广州"},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        DataSourceHelper.show(result);
        Assert.assertTrue(DataSourceHelper.equalDataSet(result, expect));
    }

    @Test
    public void testColumnInFilter() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                        {"广东", "广州"},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                });


        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ColumnInFilterOperator columnFilterOperator = FilterOperatorHelper.buildColumnInFilter("sheng", "江苏", "浙江");
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(columnFilterOperator);
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        DataSourceHelper.show(result);
        Assert.assertTrue(DataSourceHelper.equalDataSet(result, expect));
    }

    @Test
    public void testColumnNotInFilter() {

        MemoryDatasource dataSource = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"江苏", "南京"},
                        {"浙江", "宁波"},
                        {"广东", "广州"},
                });
        DataBaseContent.getInstance().addDataSource(dataSource.getName(), dataSource);


        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String},
                new String[]{"sheng", "shi"},
                new Object[][]{
                        {"广东", "广州"},
                });

        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ColumnInFilterOperator columnFilterOperator = FilterOperatorHelper.buildColumnNotInFilter("sheng", "江苏", "浙江");
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource));
        ETLOperators.add(columnFilterOperator);
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        DataSourceHelper.show(result);
        Assert.assertTrue(DataSourceHelper.equalDataSet(result, expect));
    }
}