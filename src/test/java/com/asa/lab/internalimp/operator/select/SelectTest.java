package com.asa.lab.internalimp.operator.select;


import com.asa.lab.internalimp.datasource.DataBaseContent;
import com.asa.lab.internalimp.datasource.DataSourceHelper;
import com.asa.lab.internalimp.datasource.memory.MemoryDatasource;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.relation.Relation;
import com.asa.lab.internalimp.datasource.relation.RelationBuilderHelper;
import com.asa.lab.internalimp.etl.DefaultETLBuilder;
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
 * @date 2018/11/5.
 */
public class SelectTest {

    @Before
    public void init() {

        SparkContentManager.getInstance().setDefault();
    }

    @After
    public void after() {

        SparkContentManager.getInstance().closeSession();
    }

    @Test
    public void testSelectRelationTables() {

        MemoryDatasource childTable = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.Double},
                new String[]{"table1_column", "table1_width"},
                new Object[][]{
                        {"row_a", 3.0},
                        {"row_b", 4.0},
                        {"row_c", 5.0},
                        {"row_d", 6.0},
                });


        MemoryDatasource primaryTable = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.String, Type.String, Type.Double},
                new String[]{"table2_column1", "table2_column2", "table2_width"},
                new Object[][]{
                        {"row_a", "table2_column2_2a", 7.0},
                        {"row_a", "table2_column2_2a", 8.0},
                        {"row_b", "table2_column2_2b", 9.0},
                        {"row_b", "table2_column2_2b", 10.0},
                        {"row_c", "table2_column2_2c", 11.0},
                        {"row_c", "table2_column2_2c", 12.0},
                });

        MemoryDatasource expect = DataSourceHelper.mockMemoryDatasource(
                new Type[]{Type.Double, Type.String, Type.String, Type.Double},
                new String[]{"table1_width", "table2_column1", "table2_column2", "table2_width"},
                new Object[][]{
                        {3.0, "row_a", "table2_column2_2a", 7.0},
                        {3.0, "row_a", "table2_column2_2a", 8.0},
                        {4.0, "row_b", "table2_column2_2b", 9.0},
                        {4.0, "row_b", "table2_column2_2b", 10.0},
                        {5.0, "row_c", "table2_column2_2c", 11.0},
                        {5.0, "row_c", "table2_column2_2c", 12.0},
                });

        DataBaseContent.getInstance().addDataSource(primaryTable.getName(), primaryTable);
        DataBaseContent.getInstance().addDataSource(childTable.getName(), childTable);

        StringBuffer buffer = new StringBuffer();
        buffer.append(primaryTable.getName())
                .append(RelationBuilderHelper.FIELD_SEPERATOR)
                .append("table2_column1")
                .append(RelationBuilderHelper.TABLE_SEPERATOR)
                .append(childTable.getName())
                .append(RelationBuilderHelper.FIELD_SEPERATOR)
                .append("table1_column");

        Relation relation = RelationBuilderHelper.buildSimpleRelation(buffer.toString());
        DataBaseContent.getInstance().addRelation(relation);

        SelectOperatorBuilder builder = new SelectOperatorBuilder();
        SelectOperator selectOperator = builder.addItem(childTable.getName(), "table1_width", "table1_width")
                .addItem(primaryTable.getName(), "table2_column1", "table2_column1")
                .addItem(primaryTable.getName(), "table2_column2", "table2_column2")
                .addItem(primaryTable.getName(), "table2_width", "table2_width")
                .build();

        List<ETLOperator> etlOperators = new ArrayList<ETLOperator>();
        DefaultETLBuilder etlBuilder = new DefaultETLBuilder();
        etlOperators.add(selectOperator);
        ETLJobBuilderContent content2 = new ETLJobBuilderContent();
        DataSource result = etlBuilder.build(content2, etlOperators);
        DataSourceHelper.show(result);
        Assert.assertTrue(DataSourceHelper.assertDataSource(result, expect));
    }
}