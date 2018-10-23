package com.asa.lab.internalimp.operator.union;


import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.internalimp.etl.DefaultETLBuilder;
import com.asa.lab.internalimp.operator.select.SelectOperatorHelper;
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
 * @date 2018/10/18.
 */
public class UnionTest {

    private MemoryDatasource dataSource1;

    private MemoryDatasource dataSource2;

    private MemoryDatasource dataSource3;

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
        dataSource1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"table1_column_row1", 3.0},
                        {"table1_column_row2", 4.0},
                        {"table1_column_row3", 5.0},
                        {"table1_column_row4", 6.0},
                });
        DataBaseContent.getInstance().addDataSource(dataSource1.getName(), dataSource1);


        dataSource2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table2_column", "table2_width"},
                new Object[][]{
                        {"table2_column_row1", 7.0},
                        {"table2_column_row2", 8.0},
                        {"table2_column_row3", 9.0},
                        {"table2_column_row4", 10.0},
                });
        DataBaseContent.getInstance().addDataSource(dataSource2.getName(), dataSource2);

        dataSource3 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table3_column", "table3_width"},
                new Object[][]{
                        {"table3_column_row1", 11.0},
                        {"table3_column_row2", 12.0},
                        {"table3_column_row3", 13.0},
                        {"table3_column_row4", 14.0},
                });
        DataBaseContent.getInstance().addDataSource(dataSource3.getName(), dataSource3);
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void test_UNION_NO_UNION_TABLE() {

        MemoryDatasource expect1 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "width"},
                new Object[][]{
                        {"table1_column_row1", 3.0},
                        {"table1_column_row2", 4.0},
                        {"table1_column_row3", 5.0},
                        {"table1_column_row4", 6.0},
                });

        UnionOperatorBuilder operatorBuilder = new UnionOperatorBuilder();
        UnionOperator unionOperator1 = operatorBuilder.startCurrentTalbe()
                .addCurrentItem("table1_column", "table1_width")
                .endCurrentTalbe()
                .startTableResult()
                .addResultItem("column", Type.String)
                .addResultItem("width", Type.Double)
                .endTableResult()
                .build();


        DefaultETLBuilder builder = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators = new ArrayList<ETLOperator>();
        ETLOperators.add(SelectOperatorHelper.allSelectOperator(dataSource1));
        ETLOperators.add(unionOperator1);
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder.build(content, ETLOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect1));
    }

    @Test
    public void test_UNION_SINGLE_UNION_TABLE() {

        MemoryDatasource expect2 = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"column", "width"},
                new Object[][]{
                        {"table1_column_row1", 3.0},
                        {"table1_column_row2", 4.0},
                        {"table1_column_row3", 5.0},
                        {"table1_column_row4", 6.0},
                        {"table2_column_row1", 7.0},
                        {"table2_column_row2", 8.0},
                        {"table2_column_row3", 9.0},
                        {"table2_column_row4", 10.0},
                });

        UnionOperatorBuilder operatorBuilder2 = new UnionOperatorBuilder();
        UnionOperator unionOperator2 = operatorBuilder2.startCurrentTalbe()
                .addCurrentItem("table1_column", "table1_width")
                .endCurrentTalbe()
                .startTableResult()
                .addResultItem("column", Type.String)
                .addResultItem("width", Type.Double)
                .endTableResult()
                .startUnionTables()
                .addUnionTables(dataSource2.getName(), "table2_column", "table2_width")
                .endUnionTables()
                .build();


        DefaultETLBuilder builder2 = new DefaultETLBuilder();
        List<ETLOperator> ETLOperators2 = new ArrayList<ETLOperator>();
        ETLOperators2.add(SelectOperatorHelper.allSelectOperator(dataSource1));
        ETLOperators2.add(unionOperator2);
        ETLJobBuilderContent content2 = new ETLJobBuilderContent();
        DataSource result2 = builder2.build(content2, ETLOperators2);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result2, expect2));
    }

    @Test
    public void test_UNION_SINGLE_UNION_TABLE2() {

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double, Type.String, Type.Double},
                new String[]{"table1_column", "table1_width", "table2_column", "table2_width"},
                new Object[][]{
                        {"table1_column_row1", 3.0, null, null},
                        {"table1_column_row2", 4.0, null, null},
                        {"table1_column_row3", 5.0, null, null},
                        {"table1_column_row4", 6.0, null, null},
                        {null, null, "table2_column_row1", 7.0},
                        {null, null, "table2_column_row2", 8.0},
                        {null, null, "table2_column_row3", 9.0},
                        {null, null, "table2_column_row4", 10.0},
                });

        UnionOperatorBuilder operatorBuilder2 = new UnionOperatorBuilder();
        UnionOperator unionOperator2 = operatorBuilder2.startCurrentTalbe()
                .addCurrentItem("table1_column", "table1_width", "", "")
                .endCurrentTalbe()
                .startTableResult()
                .addResultItem("column", Type.String)
                .addResultItem("width", Type.Double)
                .addResultItem("column2", Type.String)
                .addResultItem("width2", Type.Double)
                .endTableResult()
                .startUnionTables()
                .addUnionTables(dataSource2.getName(), "", "", "table2_column", "table2_width")
                .endUnionTables()
                .build();


        DefaultETLBuilder builder2 = new DefaultETLBuilder();
        List<ETLOperator> etlOperators = new ArrayList<ETLOperator>();
        etlOperators.add(SelectOperatorHelper.allSelectOperator(dataSource1));
        etlOperators.add(unionOperator2);
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder2.build(content, etlOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }

    @Test
    public void test_UNION_TWO_UNION_TABLE() {

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"table1_column_row1", 3.0},
                        {"table1_column_row2", 4.0},
                        {"table1_column_row3", 5.0},
                        {"table1_column_row4", 6.0},
                        {"table2_column_row1", 7.0},
                        {"table2_column_row2", 8.0},
                        {"table2_column_row3", 9.0},
                        {"table2_column_row4", 10.0},
                        {"table3_column_row1", 11.0},
                        {"table3_column_row2", 12.0},
                        {"table3_column_row3", 13.0},
                        {"table3_column_row4", 14.0},
                });

        UnionOperatorBuilder operatorBuilder2 = new UnionOperatorBuilder();
        UnionOperator unionOperator2 = operatorBuilder2.startCurrentTalbe()
                .addCurrentItem("table1_column", "table1_width")
                .endCurrentTalbe()
                .startTableResult()
                .addResultItem("column", Type.String)
                .addResultItem("width", Type.Double)
                .endTableResult()
                .startUnionTables()
                .addUnionTables(dataSource2.getName(), "table2_column", "table2_width")
                .addUnionTables(dataSource3.getName(), "table3_column", "table3_width")
                .endUnionTables()
                .build();

        DefaultETLBuilder builder2 = new DefaultETLBuilder();
        List<ETLOperator> etlOperators = new ArrayList<ETLOperator>();
        etlOperators.add(SelectOperatorHelper.allSelectOperator(dataSource1));
        etlOperators.add(unionOperator2);
        ETLJobBuilderContent content = new ETLJobBuilderContent();
        DataSource result = builder2.build(content, etlOperators);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }
}